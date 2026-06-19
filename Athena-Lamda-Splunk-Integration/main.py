import boto3
import time
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse
import requests
import warnings
import os
import csv

# Suppress all warnings
warnings.filterwarnings("ignore")


def send_to_splunk_hec(event_payload):
    splunk_url = "https://<<SERVER_IP OR FQDN>>:443"
    splunk_token = "<<HEC_TOKEN>>"
    
    payload = {
        "event": json.dumps(event_payload),
        "sourcetype": "<<SOURCETYPE OF YOUR CHOICE>>",
        "index": "<<INDEXNAME>>",
        "source": "<<SOURCENAME>>",
        "host": "<<HOSTNAME>>"
    }

    headers = {
        "Authorization": f"Splunk {splunk_token}"
    }

    response = requests.post(f"{splunk_url}/services/collector/event", json=payload, headers=headers, verify=False)

    if response.status_code != 200:
        print(f"Failed to send event to Splunk. Status code: {response.status_code}, Response text: {response.text}")


def count_rows(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        row_count = sum(1 for row in csv_reader)
    return row_count

# Replace below function based on end application like Slack/MS Teams/Google chat etc. ChatGPT prompt will give the code 
def post_to_webhook(message):
    webhook_url = '<<WEBHOOK URL>>'
    
    # Create a dictionary containing your message
    data = {'message': message}
    
    # Convert the dictionary to JSON format
    json_data = json.dumps(data)
    
    try:
        # Make a POST request to the webhook URL with the JSON data
        response = requests.post(webhook_url, data=json_data, headers={'Content-Type': 'application/json'})
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            print("Message posted successfully to webhook.site")
        else:
            print(f"Failed to post message. Status code: {response.status_code}")
    
    except Exception as e:
        print(f"An error occurred: {e}")


def lambda_handler(event, context):
    #******************************************************************************************************************************************************************
    RETRY_COUNT = 500
    #******************************************************************************************************************************************************************
    #******************************************************************************************************************************************************************
    #Defining time parameters to read previous day data
    #******************************************************************************************************************************************************************
    currentEpoch = int(time.time())
    #******************************************************************************************************************************************************************
    #Defining athena  parameters to perform various operations
    #******************************************************************************************************************************************************************    
    location = f"s3://<<PATH TO DATA FOLDER>>"
    print(f"location:{location}")
    database_name = '<<DATABASE NAME>>'
    table_name = f"samplecsv_{currentEpoch}" #Epoch value added to table name to keep it unique and not override any existing tables with similar name
    print(f"table name: {table_name}")
    view_name = f"{table_name}_vw"
    my_query = <<GIVE YOUR SQL QUERY>
    #my_query = f"SELECT client, status, COUNT(*) AS status_count FROM sample.samplecsv_vw GROUP BY client, status ORDER BY client, status;"
    #******************************************************************************************************************************************************************
    #Calling athena client
    #******************************************************************************************************************************************************************    
    athena_client = boto3.client('athena')
    #******************************************************************************************************************************************************************
    # Define the Athena query to create the table
    #******************************************************************************************************************************************************************  
    try:
        # Create table with all the columns mentioned here. Ref for SerDe : https://docs.aws.amazon.com/athena/latest/ug/serde-about.html
        create_table_query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {}.{}
            (
                client string,
                referrer string,
                responsesize int,
                status int,
                status_desc string,
                useragent string
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                'separatorChar' = ',',
                'quoteChar' = '\"'
            )
            LOCATION '{}';
        """.format(database_name, table_name,location)
        print(create_table_query)
        # Execute the query
        print("Starting the table create query")
        response_createTable = athena_client.start_query_execution(
            QueryString=create_table_query,
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': 's3://<<PATH TO RESULTS FOLDER>>'}
        )
        
        # Print the query execution ID
        print('Create table Query Execution ID:', response_createTable['QueryExecutionId'])    
        createTableStatus = response_createTable['ResponseMetadata']['HTTPStatusCode']
        if  createTableStatus == 200:
            print(f"table name : {table_name} created") 
    except:
        message_to_post = 'lambda failed in create table phase'
        post_to_webhook(message_to_post)

    #******************************************************************************************************************************************************************
    # Define the Athena query to create the view
    #******************************************************************************************************************************************************************     
    if  createTableStatus== 200:
        try:
            print("Giving 5 seconds sleep before creating the view")
            time.sleep(5)
            print("5 seconds sleep completed")
            # Select the columns on which analytics has to be done in the below view
            create_view_query = f"""
                CREATE OR REPLACE VIEW {database_name}.{view_name} AS
                SELECT
                    "client",
                    "referrer",
                    "responsesize",
                    "status",
                    "status_desc",
                    "useragent"
                FROM {database_name}.{table_name}; """
            # Execute the query
            response_createView = athena_client.start_query_execution(
                QueryString=create_view_query,
                QueryExecutionContext={'Database': database_name},
                ResultConfiguration={'OutputLocation': 's3://<<PATH TO RESULTS FOLDER>>'}
            )
            # Print the query execution ID
            print('Create view Query Execution ID:', response_createView['QueryExecutionId'])
            createViewStatus = response_createView['ResponseMetadata']['HTTPStatusCode']
            if  createViewStatus== 200:
                print(f"view name : {view_name} created")
        except:
            message_to_post = 'lambda failed in create view phase'
            post_to_webhook(message_to_post)

    #******************************************************************************************************************************************************************
    # Define the Athena query to query the view contents
    #******************************************************************************************************************************************************************     
    if  createViewStatus== 200:
        try:
            print("Giving 5 seconds sleep before querying the view")
            time.sleep(5)
            print("5 seconds sleep completed")
            #Start querying the athena table
            print("Starting the select query on athena....................")
            QueryResponse = athena_client.start_query_execution(
                QueryString=my_query,
                QueryExecutionContext={
                    'Database': database_name
                },
                ResultConfiguration={
                    'OutputLocation': 's3://<<PATH TO RESULTS FOLDER>>',
                }
            )
            print(QueryResponse)
            QueryId = QueryResponse['QueryExecutionId']
            queryViewStatus = QueryResponse['ResponseMetadata']['HTTPStatusCode']
            print("Status code for query the view : {queryViewStatus}")
            query_status = athena_client.get_query_execution(QueryExecutionId=QueryId)
            print(query_status)
            for i in range(1, 1 + RETRY_COUNT):
                query_status = athena_client.get_query_execution(QueryExecutionId=QueryId)
                query_execution_status = query_status['QueryExecution']['Status']['State']
                if query_execution_status == 'SUCCEEDED':
                    print("STATUS:" + query_execution_status)
                    print(query_status)
                    break
                if query_execution_status == 'FAILED':
                    #raise Exception("STATUS:" + query_execution_status)
                    print("STATUS:" + query_execution_status)

                else:
                    print("STATUS:" + query_execution_status)
                    time.sleep(i)
            else:
                # Did not encounter a break event. Need to kill the query
                athena_client.stop_query_execution(QueryExecutionId=QueryId)
                raise Exception('TIME OVER')
            
            if query_execution_status == 'SUCCEEDED':
                s3_path = query_status['QueryExecution']['ResultConfiguration']['OutputLocation']
                print("S3 bucket location where athena result is stored :" + s3_path)
                # Parse the S3 path
                parsed_url = urlparse(s3_path)
                # Extract the bucket name and key
                s3_bucket = parsed_url.netloc 
                s3_key = parsed_url.path[1:]  # Removing the leading forward slash
                print(f"Bucket Name: {s3_bucket}")
                print(f"Key: {s3_key}")
                
                # Download the CSV file from S3
                s3_client = boto3.client('s3')
                csv_file = f"/tmp/{os.path.basename(s3_key)}"
                s3_client.download_file(s3_bucket, s3_key, csv_file)
                
                # Count rows in the CSV file
                row_count = count_rows(csv_file)
                print(f"Number of rows in CSV file: {row_count}")
                
                # Read the CSV file and send rows to Splunk HEC
                with open(csv_file, 'r') as file:
                    csv_reader = csv.DictReader(file)
                    header = csv_reader.fieldnames
                    event_counter = 0
                    for row in csv_reader:
                        event_payload = {header[i]: row[header[i]] for i in range(len(header))}
                        send_to_splunk_hec(event_payload)
                        event_counter += 1
                        print(f"Event {event_counter} sent successfully to Splunk.")
        except:
            message_to_post = 'lambda failed in query the view phase'
            post_to_webhook(message_to_post)
    #******************************************************************************************************************************************************************
    # Giving 5 seconds sleep before cleanup process starts
    #******************************************************************************************************************************************************************     
    print("Sleeping for 5 seconds before termination process")
    time.sleep(5)
    tempSleep = 'completed'
    print("5 seconds sleep completed")
    #******************************************************************************************************************************************************************
    # Define the Athena query to delete the view
    #******************************************************************************************************************************************************************            
    if tempSleep == 'completed':
        try:
            drop_view_query = f"DROP VIEW IF EXISTS {database_name}.{view_name};"
            response_dropView = athena_client.start_query_execution(
                QueryString=drop_view_query,
                QueryExecutionContext={'Database': database_name},
                ResultConfiguration={'OutputLocation': 's3://<<PATH TO RESULTS FOLDER>>'}
            )
            print('Dropping View Query Execution ID:', response_dropView['QueryExecutionId'])
            dropViewStatus = response_dropView['ResponseMetadata']['HTTPStatusCode']
            if  createViewStatus== 200:
                print(f"view name : {view_name} deleted")
        except:
            message_to_post = 'lambda failed in delete view phase.'
            post_to_webhook(message_to_post)        
    #******************************************************************************************************************************************************************
    # Define the Athena query to delete the table
    #******************************************************************************************************************************************************************   
    if dropViewStatus == 200:
        print("Giving 5 seconds sleep before deleting the table")
        time.sleep(5)
        print("5 seconds sleep completed")
        try:
            drop_table_query = f"DROP TABLE IF EXISTS {database_name}.{table_name};"
            response_dropTable = athena_client.start_query_execution(
                        QueryString=drop_table_query,
                        QueryExecutionContext={'Database': database_name},
                        ResultConfiguration={'OutputLocation': 's3://<<PATH TO RESULTS FOLDER>>'}
                    )
            print('Dropping Table Query Execution ID:', response_dropTable['QueryExecutionId']) 
            dropTableStatus = response_dropTable['ResponseMetadata']['HTTPStatusCode']
            if dropViewStatus == 200:
                        print("*********LAMBDA EXECUTION SUCCESSFUL*******")
                        message_to_post = 'lambda execution completed successfully'
                        post_to_webhook(message_to_post)
        except:
            message_to_post = 'lambda failed in delete table phase.'
            post_to_webhook(message_to_post)            
