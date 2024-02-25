import time
import boto3  # used to connect with AWS services
import pandas as pd
from io import StringIO
import os

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'eu-west-2'
SCHEMA_NAME = 'covid-19'
S3_STAGING_DIR = 's3://covid-19-ukhsa/output/'
S3_BUCKET_NAME = 'covid-19-ukhsa'
S3_OUTPUT_DIRECTORY = 'output'

dict = {}

def download_and_load_query_athena(client: boto3.client, query_response: dict) -> pd.DataFrame:
    while True:
        try:
            client.get_query_results(
                QueryExecutionId=query_response['QueryExecutionId']
            )
            break
        except Exception as e:
            if 'not finish' in str(e) or 'not yet finished' in str(e):
                time.sleep(0.001)
            else:
                raise e
    temp_file_location: str = 'athena_query_results.csv'
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=AWS_REGION,
                      )
    s3.download_file(
        S3_BUCKET_NAME, f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)


table_list = ['enigma_jhu', 'us_daily', 'us_states', 'us_total_latest', 'states_abv',
              'rearc_usa_hospital_beds', 'county_population', 'countrycode', 'us_county', 'states_daily']
athena = boto3.client('athena',
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=AWS_REGION,
                      )
table_dict = {}
for table_name in table_list:
    response = athena.start_query_execution(
        QueryString=f'SELECT * FROM {table_name}',
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            'OutputLocation': S3_STAGING_DIR,
            'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'},
        },
    )
    table_dict[table_name] = download_and_load_query_athena(athena, response)
    if table_name == 'states_abv':
        header = table_dict[table_name].iloc[0]
        table_dict[table_name] = table_dict[table_name][1:]
        table_dict[table_name].columns = header
    # print(table_name.upper())
    # print(table_dict[table_name])

# factCovid_1 = pd.merge(table_dict['enigma_jhu'], table_dict['states_abv'], left_on='province_state', right_on='State', how='inner')
# print(factCovid_1)
# factCovid_2 = table_dict['states_daily'][['fips', 'state', 'date', 'positive', 'negative', 'hospitalizedcurrently',
#                                           'hospitalized', 'hospitalizeddischarged', 'recovered', 'death']]
# factCovid_2 = factCovid_2.rename(columns={'state': 'Abbreviation'})
# print(factCovid_2)
# factCovid = pd.merge(factCovid_1, factCovid_2, on='Abbreviation', how='inner')
# # print('factCovid'.upper(),factCovid.head())

factCovid_1 = table_dict['enigma_jhu'][['fips', 'province_state', 'country_region', 'confirmed', 'deaths', 'recovered', 'active']]
factCovid_2 = table_dict['states_daily'][['fips', 'date', 'positive', 'negative', 'hospitalizedcurrently',
                                           'hospitalized', 'hospitalizeddischarged', 'recovered', 'death']]
factCovid = pd.merge(factCovid_1, factCovid_2, on='fips', how= 'inner')

dimRegion_1 = table_dict['enigma_jhu'][['fips', 'province_state', 'country_region', 'latitude', 'longitude']]
dimRegion_2 = table_dict['us_county'][['fips', 'county', 'state']]
dimRegion = pd.merge(dimRegion_1, dimRegion_2, on='fips', how='inner')
# print('dimRegion'.upper(),dimRegion.head())

dimHospital = table_dict['rearc_usa_hospital_beds'][['fips', 'state_name', 'latitude', 'longtitude', 'hq_address', 'hospital_type', 'hospital_name', 'hq_city', 'hq_state']]
print('dimHospital'.upper(),dimHospital.head())

dimDate = table_dict['states_daily'][['fips', 'date']]
dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')
dimDate.loc[:, 'year'] = dimDate['date'].dt.year
dimDate.loc[:, 'month'] = dimDate['date'].dt.month
dimDate.loc[:, 'day_of_week'] = dimDate['date'].dt.dayofweek
# print('dimDate'.upper(),dimDate.head())

csv_buffers = {
    # 'factCovid': StringIO(),
    'dimRegion': StringIO(),
    'dimHospital': StringIO(),
    'dimDate': StringIO()
}

s3_resource = boto3.resource('s3')

for table_name, buffer in csv_buffers.items():
    dataframe = globals()[table_name]
    dataframe.to_csv(buffer)
    s3_resource.Object(S3_BUCKET_NAME, f'final-output/{table_name}.csv').put(Body=buffer.getvalue())

factCovidSQL = pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')
factCovidSQL = ''.join(factCovidSQL)

dimRegionSQL = pd.io.sql.get_schema(dimRegion.reset_index(), 'dimRegion')
dimRegionSQL = ''.join(dimRegionSQL)

dimHospitalSQL = pd.io.sql.get_schema(dimHospital.reset_index(), 'dimHospital')
dimHospitalSQL = ''.join(dimHospitalSQL)

dimDateSQL = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
dimDateSQL = ''.join(dimDateSQL)

client = boto3.client('redshift-data')


# Define parameters for the other SQL statements and execute them sequentially
sql_statements = {
    'factCovidSQL': factCovidSQL,
    'dimRegionSQL': dimRegionSQL,
    'dimHospitalSQL': dimHospitalSQL,
    'dimDateSQL': dimDateSQL
}

for statement_name, sql_statement in sql_statements.items():
    # Define the parameters for the execute statement
    parameters = {
        'Database': 'dev',
        'Sql': sql_statement,
        'WorkgroupName': 'ukhsa-redshift-cluster'
    }

    # Execute the SQL query
    response = client.execute_statement(**parameters)

    # Retrieve the query ID
    query_id = response['Id']

    # Poll for query completion
    while True:
        # Describe the statement to get status
        query_status = client.describe_statement(Id=query_id)['Status']

        if query_status == 'FINISHED':
            break
        elif query_status == 'FAILED':
            raise Exception(f"Query execution failed: {client.describe_statement(Id=query_id)['Error']}")

        # Sleep for a short interval before polling again
        time.sleep(2)

    # Optionally, you can check the status of the execution to ensure it was successful
    execution_status = client.describe_statement(Id=query_id)['Status']

    if execution_status == 'FINISHED':
        print(f"{statement_name} execution successful!")
    else:
        print(f"{statement_name} execution failed!")


# Define COPY statements for each table
copy_sql = {
    'factCovidCopy':"""
        COPY factCovid FROM 's3://covid-19-ukhsa/final-output/factCovid.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::211125699284:role/redshift-s3-access'
        DELIMITER ','
        REGION 'eu-west-2'
        IGNOREHEADER 1
    """,
    'dimRegionCopy': """
        COPY dimRegion FROM 's3://covid-19-ukhsa/final-output/dimRegion.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::211125699284:role/redshift-s3-access'
        DELIMITER ','
        REGION 'eu-west-2'
        IGNOREHEADER 1
    """,
    'dimHospitalCopy': """
        COPY dimHospital FROM 's3://covid-19-ukhsa/final-output/dimHospital.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::211125699284:role/redshift-s3-access'
        DELIMITER ','
        REGION 'eu-west-2'
        IGNOREHEADER 1
    """,
    'dimDateCopy': """
        COPY dimDate FROM 's3://covid-19-ukhsa/final-output/dimDate.csv'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::211125699284:role/redshift-s3-access'
        DELIMITER ','
        REGION 'eu-west-2'
        IGNOREHEADER 1
    """
}

# Execute COPY statements
for statement_name, sql_statement in copy_sql.items():
    # Define the parameters for the execute statement
    parameters = {
        'Database': 'dev',
        'Sql': sql_statement,
        'WorkgroupName': 'ukhsa-redshift-cluster'
    }

    # Execute the SQL query
    response = client.execute_statement(**parameters)

   # Retrieve the query ID
    query_id = response['Id']

    # Poll for query completion
    while True:
        # Describe the statement to get status
        query_status = client.describe_statement(Id=query_id)['Status']

        if query_status == 'FINISHED':
            break
        elif query_status == 'FAILED':
            raise Exception(f"Query execution failed: {client.describe_statement(Id=query_id)['Error']}")

        # Sleep for a short interval before polling again
        time.sleep(2)

    print(f"{statement_name} execution successful!")