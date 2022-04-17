#library donwloads
import requests
import json
import boto3
from datetime import datetime
import pickle


#parameters to download releavant data 
cuisineTypes = ['italian','american','japanese','chinese','mexican']
location = ['brooklyn', 'manhattan', 'bronx', 'queens', 'staten island']
businessType = 'restaurant'
radius ='40000'

#aws credentials (Please enter your credentials)
#removed for security reasons
awsRegion = 'us-east-1'
accessID=''
secretKey = ''
dbObject = boto3.resource('dynamodb',region_name=awsRegion, aws_access_key_id=accessID, aws_secret_access_key=secretKey)
dbtable = dbObject.Table('yelp-restaurants')

#yelp configuration
yelpEndpoint='https://api.yelp.com/v3/businesses/search'
clientID=''
yelpKey=''
yelpConfig={'Authorization':'Bearer ' + yelpKey}

globalDict = {}
opensearchDict = {}

#insert data in the dictionary in the desired format 
def updateDict(responseObject, cuisineType):
    print(responseObject)
    try:
        restaurentList = responseObject['businesses']
        if(len(restaurentList)>0):
                for i in range(len(restaurentList)):
                    tmpKey = restaurentList[i]['id']
                    if tmpKey in globalDict.keys():
                        continue
                    else:
                        tmpValue = {
                                'id': restaurentList[i]['id'],
                                'restaurent_name': restaurentList[i]['name'],
                                'address': restaurentList[i]['location']['display_address'],
                                'zip_code' : restaurentList[i]['location']['zip_code'],
                                'timestamp': datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                            }
                        globalDict[tmpKey] = tmpValue
                        opensearchTmpValue = {'id': restaurentList[i]['id'],'cuisine':cuisineType}
                        opensearchDict[tmpKey] = opensearchTmpValue
    except:
        print('')

#collect data from yelp using below parameters
def yelpDataCollecter():
    for cuisine in cuisineTypes:
        for loc in location:
            offsetValue = 1
            for i in range(18):
                urlParameter = {'categories':cuisine,'location':loc, 'limit':50, 'offset':offsetValue,'radius': radius}
                apiResponse = requests.get(yelpEndpoint, headers = yelpConfig, params = urlParameter)
                jsonResponse = json.loads(apiResponse.content.decode("utf-8"))
                updateDict(jsonResponse,cuisine)
                offsetValue = offsetValue + 50

#insert into dynamo db using batch write operation
def dbInsert(restaurentDict):
    with dbtable.batch_writer() as writer:
        for validKey in restaurentDict:
            writer.put_item(
                Item=restaurentDict[validKey]
   
            )
#save data to your local file
def saveData(gDict, osDict):
    f = open("dynamo.pkl","wb")
    pickle.dump(gDict,f)
    f.close()

    q = open("opensearch.pkl","wb")
    pickle.dump(osDict,q)
    q.close()

yelpDataCollecter()
print("Items in global dictionary:", len(globalDict))
print("Items in global dictionary:", len(opensearchDict))

saveData(globalDict, opensearchDict)
userInput = int(input("Update DynamoDb and Open Search?"))

if(userInput == 1):
    dbInsert(globalDict)
else:
    print("Ending program")
