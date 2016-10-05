import boto3,time
from boto3.dynamodb.conditions import Key, Attr
from textblob import TextBlob
import google_translate
from decimal import *
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
# items = response['Items']
# print(items)
s_time = 1474369886 
one_day = 60*60*24
terminate_time = s_time - one_day
endtime = s_time
while endtime > terminate_time:
	
	pe = "tweetid,epoch,#s"
	ean = { "#s": "status"}
	response = table.scan(
	FilterExpression = Attr('epoch').between(endtime-200, endtime),
	ProjectionExpression = pe,
	ExpressionAttributeNames = ean
	)
	endtime -= 201
	items = response['Items']
	#print(len(items))
	translator = google_translate.GoogleTranslator()
	detected = {}
	before_trans = ""
	sentiment_score = {}
	keyw = {}
	for item in items:
		tid = item["tweetid"]
		input_s = item["status"]
		lang = translator.detect(input_s)
		trans = False
		if(lang != None):
			# remove hashtag for translation convenience
			status = input_s.replace("#", "")
			if(lang != "english"):
				# print(status)
				trans = True
				status = translator.translate(input_s,"english")

			if(len(status) >0 ):
				# print(status)
				testimonial = TextBlob(status)
				for np in testimonial.noun_phrases:
						if np in keyw.keys():
							keyw[np].add(tid) 
						else:
							keyw[np]=set([tid])
				
				sentiment_score = int(testimonial.sentiment.polarity*5 +5)
				# if sentiment_score[tid] == None: sentiment_score[tid]=5
				# print(sentiment_score[tid])
				# print(status)
				if trans:
					# print(tid)
					try:
						table.update_item(
						    Key={
						        'tweetid': tid,
						    },
						    UpdateExpression='SET lang= :val1 , status_en=:val2 , sentiment = :val3',
						    ExpressionAttributeValues={
						        ':val1': lang,
						        ':val2': status,
						        ':val3': sentiment_score
						    }
						)
					except:
						print(tid)
				else:
					try:
						table.update_item(
						    Key={
						        'tweetid': tid,
						    },
						    UpdateExpression='SET lang= :val1 ,sentiment = :val3',
						    ExpressionAttributeValues={
							':val1': lang,
						    ':val3': sentiment_score
						    }
						)
					except:
						print(tid)


	rtable = dynamodb.Table('keyword')
	# print(rtable.creation_date_time)
	project = "tweetid"
	
	with rtable.batch_writer() as batch:
		for k,v in keyw.items():
			#time.sleep(15)
			ctime = long(time.time())
			# print(k)
			res = rtable.query(KeyConditionExpression=Key('keyword').eq(k), ProjectionExpression = project)
			
			res_item=res['Items']
			
			#if key doesn't exist, put new item directly
			if len(res_item)==0:
				length = len(v)
				try:
					rtable.put_item(
					Item={
					'keyword': k,
					'tweetid': v,
					'epoch': ctime,
					'count': length})
				except:
						print(v)
			#union exsiting set and new set. set new len = len(new_set)
			else:
				# print(res_item)
				exist_set = res_item[0]["tweetid"]
				new_set = exist_set | v
				length = len(new_set)
				try:
					rtable.put_item(
						Item={
						'keyword': k,
						'tweetid': new_set,
						'epoch': ctime,
						'count': length})
				except:
						print(v)
	time.sleep(60)
