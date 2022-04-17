#import libraries
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import pickle
import random

#aws credentials (Please enter your credentials)
#removed for security reasons
openSearchEndpoint = 'https://search-restaurants-g7ikxqgqdbmkt64tdwa7fci6v4.us-east-1.es.amazonaws.com' 
region = 'us-east-1'
accessID=''
secretKey = ''

#creating a OpenSearch connection 
service = 'es'
credentials = boto3.Session(region_name=region, aws_access_key_id=accessID, aws_secret_access_key=secretKey).get_credentials()
awsauth = AWS4Auth(accessID, secretKey, region, service, session_token=credentials.token)

#creating new search query
search = OpenSearch(
    hosts = openSearchEndpoint,
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

index_body = {
  'settings': {
    'index': {
      'number_of_shards': 4
    }
  }
}

#creates a index in the Open search as restbot
response = search.indices.create('restbot', body=index_body)

print('\nCreating index:')
print(response)

#load the data from the pickle file
pickle_file = open("opensearch.pkl", "rb")
oDict = pickle.load(pickle_file)

id = 1

#adding document into opensearch
for tmpKey in oDict.keys():
    print(tmpKey)
    print(oDict[tmpKey])
    try:        
        response = search.index(
            index = 'restbot',
            body = {
                'restaurantID': tmpKey,
                'cuisine': oDict[tmpKey]['cuisine']
            },
            refresh = True
            )
        id = id + 1
        print('\nAdding document:')
        print(response)
    except:
        print('')