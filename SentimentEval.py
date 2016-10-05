import boto3,time,re,decimal
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

# print(table.creation_date_time)
# print(int(time.time()))
# response = table.query(
#      KeyConditionExpression=Key('tweetid').eq(778203325023989761)
#  )
#  items = response['Items']
# print(items)
pe = "tweetid,epoch,#s"
ean = { "#s": "status"}
response = table.scan(
    FilterExpression = Attr('epoch').gte(1474381105),
    ProjectionExpression = pe,
    ExpressionAttributeNames = ean
)
items = response['Items']

translator = google_translate.GoogleTranslator()
detected = {}
translated = {}
sentiment_score = {}
keyw = {}
for item in items:
	tid = item["tweetid"]
	input_s = item["status"]
	lang = translator.detect(input_s)
	if(lang != None):
		# remove hashtag for translation convenience
		status = input_s.replace("#", "")
		if(lang != "english"):
			# print(status)
			status = translator.translate(input_s,"english")
			detected[tid]=lang
			translated[tid]=status
		if(status!= None):
			# print(status)
			testimonial = TextBlob(status)
			for np in testimonial.noun_phrases:
					if np in keyw.keys():
						keyw[np].add(long(tid)) 
					else:
						keyw[np]=set([long(tid)])
			
			sentiment_score[tid] = testimonial.sentiment.polarity
#print(items)
# print keyw
rtable = dynamodb.Table('keyword')
# print(rtable.creation_date_time)
project = "tweetid"
with rtable.batch_writer() as batch:
    for k,v in keyw.items():
    	
		ctime = long(time.time())
		print(k)
		res = rtable.query(KeyConditionExpression=Key('keyword').eq(k), ProjectionExpression = project)
		
		res_item=res['Items']
		
		if(len(res_item)==0):
			length = len(v)
			rtable.put_item(
			Item={
			'keyword': k,
			'tweetid': v,
			'epoch': ctime,
			'count': length})
		else:
			# print(res_item)
			exist_set = res_item[0]["tweetid"]
			new_set = exist_set | v
			length = len(new_set)
			rtable.put_item(
				Item={
				'keyword': k,
				'tweetid': new_set,
				'epoch': ctime,
				'count': length})

print(len(items))