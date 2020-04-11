import boto3
import csv
import os
import sys
import uuid
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
dynamodb = boto3.client('dynamodb', region_name='us-east-1')
# table = dynamodb.Table('result')

BUCKET = 'covid-tracker-801101744'
KEY = 'handle-csv/'


def main(event, context, KEY=KEY):

    
    if event:
        print("event-----",event)
        file_obj = event["Records"][0]
        filename = str(file_obj['s3']['object']['key'])
        fileObj = s3_client.get_object(Bucket=BUCKET, Key=filename)
        key = unquote_plus(file_obj['s3']['object']['key'])
        print(key)
        tmpkey = key.replace('/', '')
        print(tmpkey)
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        print(download_path)
        upload_path = '/tmp/{}'.format(tmpkey)
        print(upload_path)
        s3_client.download_file(BUCKET, key, download_path)

        existing_tables = dynamodb.list_tables()['TableNames']

        table_name = ''

        if '/' in filename:
            temp_list = filename.split("/")
            table_name = temp_list[1][:-4]
        else:
            table_name = filename[:-4]

        if table_name not in existing_tables:
            dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'place',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'region',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'place',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'region',
                        'AttributeType': 'S'
                    },

                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

        table = boto3.resource('dynamodb').Table(table_name)

        with open(download_path, newline='') as f:
            reader = csv.reader(f)
            data = list(reader)
            for i in data:
                if i[8] == 'global' and i[0] != "":
                    response = table.get_item(
                        Key={
                            'place': str(i[0]),
                            'region': 'global'
                        }
                    )        
                    if 'Item' not in response.keys():
                        table.put_item(
                            Item={
                                "place": i[0],
                                "cases": i[1],
                                "new_cases": i[2],
                                "total_cases": i[3],
                                "deaths": i[4],
                                "new_deaths": i[5],
                                "total_deaths": i[6],
                                "recovered": i[7],
                                "region": i[8],
                            }
                        )
                    else:
                        table.update_item(
                            Key={
                                'place': str(i[0]),
                                'region': 'global'
                            },
                            UpdateExpression='SET cases = :val1, new_cases = :val2, total_cases = :val3, deaths = :val4, new_deaths = :val5, total_deaths = :val6, recovered = :val7',
                            ExpressionAttributeValues={
                                ':val1': i[1],
                                ':val2': i[2],
                                ':val3': i[3],
                                ':val4': i[4],
                                ':val5': i[5],
                                ':val6': i[6],
                                ':val7': i[7],
                            }
                        )
    
    # print("Table status:", table.table_status)

    print('All done !')


if __name__ == "__main__":
    main('', '')
