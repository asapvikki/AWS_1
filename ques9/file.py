import boto3
import time
import logging

#Initialize the logger
logging.basicConfig(filename="q9.log", 
                    format='%(asctime)s %(message)s', 
                    filemode='w') 
#Creating an object 
logger=logging.getLogger() 
#set logger to debug
logger.setLevel(logging.DEBUG) 

def create_games_table(dynamodb_client):
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'gid',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'gname',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'rating',
                'AttributeType': 'N'
            },
        ],
        TableName='vyGamesTable',
        KeySchema=[
            {
                'AttributeName': 'gid',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'gname',
                'KeyType': 'RANGE'
            },
        ],
        LocalSecondaryIndexes=[
            {
                'IndexName': 'gid_rating_index',
                'KeySchema': [
                    {
                        'AttributeName': 'gid',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'rating',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
            },
        ],
        BillingMode='PROVISIONED',
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )


def add_items(dynamodb_client):
    response1 = dynamodb_client.put_item(
        TableName='vyGamesTable',
        Item={
            'gid': {
                'N': "9999999",
            },
            'gname': {
                'S': "last",
            },
            'publisher': {
                'S': "arihant",
            },
            'rating': {
                'N': "33456",
            },
            'release_date': {
                'S': "2019/01/06",
            }
        }
    )
    response2 = dynamodb_client.put_item(
        TableName='vyGamesTable',
        Item={
            'gid': {
                'N': "1000000",
            },
            'gname': {
                'S': "secondlast",
            },
            'rating': {
                'N': "4123",
            },
            'release_date': {
                'S': "2019/01/02",
            },
            'genres': {
                'SS': ['gc', 'gb'],
            }
        }
    )
    response3 = dynamodb_client.put_item(
        TableName='vyGamesTable',
        Item={
            'gid': {
                'N': "2",
            },
            'gname': {
                'S': "Third",
            },
            'publisher': {
                'S': "pblisher",
            },
            'rating': {
                'N': "2",
            },
            'release_date': {
                'S': "2019/01/06",
            },
            'genres': {
                'SS': ['gd'],
            }
        }
    )
    print("Added items to table")


if __name__ == '__main__':
    dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    #comment add_items while creating table and uncomment the next line
    #create_games_table(dynamodb_client)
    add_items(dynamodb_client)

