import boto3,time
from boto3.dynamodb.conditions import Key, Attr
from textblob import TextBlob
import google_translate
# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Instantiate a table resource object without actually
# creating a DynamoDB table. Note that the attributes of this table
# are lazy-loaded: a request is not made nor are the attribute
# values populated until the attributes
# on the table resource are accessed or its load() method is called.
table = dynamodb.Table('Tweets')

# Print out some data about the table.
# This will cause a request to be made to DynamoDB and its attribute
# values will be set based on the response.

print(table.creation_date_time)
print(int(time.time()))
response = table.query(
    KeyConditionExpression=Key('tweetid').eq(778203325023989761)
)
items = response['Items']
print(items)
pe = "tweetid,epoch,#s"
ean = { "#s": "status"}
response = table.scan(
    FilterExpression=Attr('epoch').gte(1474381105),
    ProjectionExpression=pe,
     ExpressionAttributeNames=ean
)
items = response['Items']

translator = google_translate.GoogleTranslator()
detected = {}
translated = {}
for item in items:
	tid = item["tweetid"]
	input_s = item["status"]
	lang = translator.detect(input_s)
	if(lang != None):
		status = input_s
		if(lang != "english"):
			print(status)
			status = translator.translate(input_s,"english")
			detected[tid]=lang
			translated[tid]=status
		if(status!= None):
			print(status)
			testimonial = TextBlob(status)
			print(testimonial.sentiment.polarity)
#print(items)
print(len(items))