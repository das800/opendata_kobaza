'''
Dayan Siddiqui
2021-01-17

purpose: provide abstraction for accessing the datasets and their metavars

reqs: needs aws cli to be installed and configured in the same env to work properly
'''
###with aws dynamobd (make sure aws cli is installed and configured)
import boto3
from boto3.dynamodb.conditions import Key, Attr

aws_db_service = 'dynamodb'
aws_region_id = 'eu-central-1'
aws_dynamodb_tablename = 'kobaza_ds_metavars'

dynamodbres = boto3.resource(aws_db_service, region_name = aws_region_id)
dynamodbclient = boto3.client(aws_db_service, region_name = aws_region_id)
table = dynamodbres.Table(aws_dynamodb_tablename)


def get_item_by_ds_id(ds_id):
	'''
	retreives a single ds_metavar item by its ds_id
	'''
	item = table.get_item(Key = {'ds_id': ds_id})['Item']

	return item


def get_all_ds_ids():
	'''
	gets all prim keys for ds metavars from the db, TODO never use this as scan is expensive and you should never retrieve your whole db
	'''
	scan_paginator = dynamodbclient.get_paginator('scan')

	scan_paginate_params = {
		'TableName': aws_dynamodb_tablename,
		'ProjectionExpression': 'ds_id',
	}
	scan_iterator = scan_paginator.paginate(**scan_paginate_params)

	all_ds_ids = []
	for page in scan_iterator:
		for item in page['Items']:
			all_ds_ids.append(item['ds_id']['S'])

	return all_ds_ids


def get_names_by_ds_ids(ds_ids):
	'''
	takes in a list of ds_ids and return a projection from the db of corresponding names
	'''

	ds_names_dict = {}
	for ds_id in ds_ids:
		query_params = {
			'ProjectionExpression': "ds_id, #nm",
			'ExpressionAttributeNames': {"#nm": "name"},
			'KeyConditionExpression': Key('ds_id').eq(ds_id)
		}
		response = table.query(**query_params)
		ds_names_dict[ds_id] = response['Items'][0]['name']

	return ds_names_dict

