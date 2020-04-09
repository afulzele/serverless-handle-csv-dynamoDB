import boto3
import numpy as np
import csv

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
dynamodb = boto3.client('dynamodb', region_name='us-east-1')
# table = dynamodb.Table('Movies')

BUCKET = 'covid-tracker-801101744'
KEY = 'handle-csv/'


def main(event, context, KEY=KEY):
    all_files = []
    storing_file_names = []

    # ---------------------------S3-----------------------------

    my_bucket = s3_resource.Bucket(BUCKET)
    for file in my_bucket.objects.filter(Prefix=KEY):
        if file.key != KEY:
            storing_file_names.append(file.key)
            temp_list = file.key.split("/")
            all_files.append(temp_list[1][:-4])

    existing_tables = dynamodb.list_tables()['TableNames']

    required_list = list(np.setdiff1d(all_files, existing_tables))

    # ---------------------------CREATE A TABLE-----------------------------

    if len(required_list) > 0:
        for i in required_list:
            table = dynamodb.create_table(
                TableName=i,
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

    # --------------------------- Updating else creating in table-----------------------------

    for k in storing_file_names:
        print(k)
        local_file_name = '/tmp/' + k
        s3_resource.Bucket(BUCKET).download_file(k, local_file_name)

    # with open(KEY, newline='') as f:
    #     reader = csv.reader(f)
    #     data = list(reader)
    #     for i in data:
    #         if i[8] == 'global' and i[0] != "":
    #             response = table.get_item(
    #                 Key={
    #                     'place': str(i[0]),
    #                     'region': 'global'
    #                 }
    #             )
    #
    #             if 'Item' not in response.keys():
    #                 table.put_item(
    #                     Item={
    #                         "place": i[0],
    #                         "cases": i[1],
    #                         "new_cases": i[2],
    #                         "total_cases": i[3],
    #                         "deaths": i[4],
    #                         "new_deaths": i[5],
    #                         "total_deaths": i[6],
    #                         "recovered": i[7],
    #                         "region": i[8],
    #                     }
    #                 )
    #             else:
    #                 table.update_item(
    #                     Key={
    #                         'place': str(i[0]),
    #                         'region': 'global'
    #                     },
    #                     UpdateExpression='SET cases = :val1, new_cases = :val2, total_cases = :val3, deaths = :val4, new_deaths = :val5, total_deaths = :val6, recovered = :val7',
    #                     ExpressionAttributeValues={
    #                         ':val1': i[1],
    #                         ':val2': i[2],
    #                         ':val3': i[3],
    #                         ':val4': i[4],
    #                         ':val5': i[5],
    #                         ':val6': i[6],
    #                         ':val7': i[7],
    #                     }
    #                 )

    # --------------------------- Uploading in table-----------------------------

    # with open(KEY, newline='') as f:
    #     reader = csv.reader(f)
    #     data = list(reader)
    #     for i in data:
    #         print('----------------------------------', i)
    #         if i[8] == 'United States' and i[0] != "":
    #             table.put_item(
    #                 Item={
    #                     "place": i[0],
    #                     "cases": i[1],
    #                     "new_cases": i[2],
    #                     "total_cases": i[3],
    #                     "deaths": i[4],
    #                     "new_deaths": i[5],
    #                     "total_deaths": i[6],
    #                     "recovered": i[7],
    #                     "region": i[8],
    #                 }
    #             )

    #   print("Table status:", table.table_status)

    print('All done !')


if __name__ == "__main__":
    main('', '')
