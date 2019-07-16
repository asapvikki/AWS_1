import boto3
from boto3.dynamodb.conditions import Key
import logging

#Initialize the logger
logging.basicConfig(filename="q10.log", 
                    format='%(asctime)s %(message)s', 
                    filemode='w') 
#Creating an object 
logger=logging.getLogger() 
#set logger to debug
logger.setLevel(logging.DEBUG) 

def make_query():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('vyGamesTable')
    gid = 2
    query_response = table.query(
        IndexName='gid_rating_index',
        Select='ALL_PROJECTED_ATTRIBUTES',
        KeyConditionExpression=Key('gid').eq(gid)
    )
    for item in query_response['Items']:
        print("gname: {}, rating: {}".format(item['gname'], item['rating']))


if __name__ == '__main__':
    make_query()
