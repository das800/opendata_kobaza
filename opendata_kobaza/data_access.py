'''
Dayan Siddiqui
2021-01-17

purpose: provide abstraction for accessing storing the datasets and their metavars in all stores

reqs: needs aws cli to be installed and configured in the same env to work properly
'''
###with aws dynamobd (make sure aws cli is installed and configured)
import json
import os
from main import app
import requests
import boto3
import uuid
from boto3.dynamodb.conditions import Key, Attr
from werkzeug.utils import secure_filename
import werkzeug

#custom imports
import kobaza_error
import search_kobaza

ALLOWED_EXTENSIONS = ['csv', 'tsv']
DS_FOLDER = os.path.join(app.root_path, 'datasets', 'sets')

aws_db_service = 'dynamodb'
aws_region_id = 'eu-central-1'
aws_dynamodb_tablename = 'kobaza_ds_metavars'

dynamodbres = boto3.resource(aws_db_service, region_name = aws_region_id)
dynamodbclient = boto3.client(aws_db_service, region_name = aws_region_id)
table = dynamodbres.Table(aws_dynamodb_tablename)




#metavarset integrity functions
def jsonify(js):
	'''
	make sure $metaver_json is a json (dict), must not pass in json as a string
	'''
	if isinstance(js, str) and js:
		js = json.loads(js)
	return js

def is_valid_metavarset(metavar_json, verbose = False):
	'''
	validates that $metavar_json is the correct format for metavar json
	'''
	if not isinstance(metavar_json, dict):
		if verbose:
			print(1)
		return False
	elif not (set(metavar_json.keys()) == {'meta-attributes', 'ds_id', 'raw data file', 'ds_source', 'last updated', 'name', 'cleaned data file'}):
		if verbose:
			print(2)
		return False
	elif not isinstance(metavar_json['meta-attributes'], dict):
		if verbose:
			print(3)
		return False
	elif not (set(metavar_json['meta-attributes'].keys()) == {'context', 'size', 'variables'}):
		if verbose:
			print(4)
		return False
	elif not isinstance(metavar_json['meta-attributes']['context'], dict):
		if verbose:
			print(5)
		return False
	elif not (set(metavar_json['meta-attributes']['context'].keys()) == {'process', 'situation', 'domain'}):
		if verbose:
			print(6)
		return False
	elif not isinstance(metavar_json['meta-attributes']['size'], dict):
		if verbose:
			print(7)
		return False
	elif not (set(metavar_json['meta-attributes']['size'].keys()) == {'rows', 'description'}):
		if verbose:
			print(8)
		return False
	elif not isinstance(metavar_json['meta-attributes']['variables'], dict):
		if verbose:
			print(9)
		return False
	elif not (set(metavar_json['meta-attributes']['variables'].keys()) == {'variable_names', 'variable_descriptions'}):
		if verbose:
			print(10)
		return False
	elif not (isinstance(metavar_json['meta-attributes']['context']['process'], str) & isinstance(metavar_json['meta-attributes']['context']['situation'], str) & isinstance(metavar_json['meta-attributes']['context']['domain'], str)):
		if verbose:
			print(11)
		return False
	elif not (isinstance(metavar_json['meta-attributes']['size']['rows'], int) & isinstance(metavar_json['meta-attributes']['size']['description'], str)):
		if verbose:
			print(12)
		return False
	elif not (isinstance(metavar_json['meta-attributes']['variables']['variable_names'], list) & isinstance(metavar_json['meta-attributes']['variables']['variable_descriptions'], list)):
		if verbose:
			print(13)
		return False
	elif not (len(metavar_json['meta-attributes']['variables']['variable_names']) == len(metavar_json['meta-attributes']['variables']['variable_descriptions'])):
		if verbose:
			print(14)
		return False

	return True




### DYNAMODB FUNCTIONS

def get_item_by_ds_id_dynamodb(ds_id):
	'''
	retreives a single ds_metavar item by its $ds_id
	'''
	response = table.get_item(Key = {'ds_id': ds_id})
	response = jsonify(response)

	if 'Item' in list(response.keys()):
		response['Item']['meta-attributes']['size']['rows'] = int(response['Item']['meta-attributes']['size']['rows'])
		return response['Item']
	else:
		raise kobaza_error.MetavarsetNotFoundError(ds_id)


# def get_item_by_ds_id_test(ds_id):
# 	'''
# 	retreives a single ds_metavar item by its $ds_id
# 	'''
# 	response = table.get_item(Key = {'ds_id': ds_id})
# 	response['Item']['meta-attributes']['size']['rows'] = int(response['Item']['meta-attributes']['size']['rows'])
# 	return json.dumps(response, indent = 4)


def get_all_ds_ids_dynamodb():
	'''
	gets all prim keys for ds metavars from the db, note never use this as scan is expensive and you should never retrieve your whole db
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


def get_names_by_ds_ids_dynamodb(ds_ids):
	'''
	takes in a list of $ds_ids and return a projection from the db of corresponding names
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

def confirm_db_response(response, purpose, ds_id):
	'''
	confirm than the $response recieved from the dynamodb operation on $ds_id was successful for its $purpose
	param:
		purpose: must be one of ('created', 'deleted')
	'''
	assert purpose in ['created', 'deleted']
	response = jsonify(response)

	if response['ResponseMetadata']['HTTPStatusCode'] == 200:
		if purpose == 'created':
			try:
				item = get_item_by_ds_id_dynamodb(ds_id)
				if is_valid_metavarset(item): #exists so was created
					return True
			except kobaza_error.MetavarsetNotFoundError as e: #does not exist so was not created
				return False
		elif purpose == 'deleted':
			try:
				item = get_item_by_ds_id_dynamodb(ds_id)
				if is_valid_metavarset(item): #exists so was not deleted
					return False
			except kobaza_error.MetavarsetNotFoundError as e: #does not exist so was deleted
				return True
	else:
		return False


def insert_metavarset_dynamodb(metavar_json):
	'''
	takes in a json, $metavar_json, and uploads it to the dynamodb
	'''
	metavar_json = jsonify(metavar_json)
	
	if is_valid_metavarset(metavar_json):
		response = table.put_item(Item = metavar_json)
	else:
		raise kobaza_error.MetavarsetIsInvalid(metavar_json)
	return confirm_db_response(response, 'created', metavar_json['ds_id'])

def delete_metavarset_dynamodb(ds_id):
	'''
	delete metavarset with id $ds_id from dynamodb
	'''
	response = table.delete_item(Key = {'ds_id': ds_id})
	return confirm_db_response(response, 'deleted', ds_id)
	return response





### ELASTICSEARCH FUNCTIONS

def read_creds(filename):
	'''
	reads elasticsearch creds from $filename assuming 1st, 2nd, 3rd lines are endpoint, u, and p respectively
	'''
	with open(filename, 'r') as creds_fh:
		endpoint = creds_fh.readline().strip()
		username = creds_fh.readline().strip()
		password = creds_fh.readline().strip()
	return endpoint, username, password


def elasticsearch_curl(es_endpoint, es_username, es_password, json_body = '', verb = 'get'):
	'''constructs curl like command from requests library, $json_body must be string representing json'''

	# pass header option for content type if request has a body to avoid Content-Type error in Elasticsearch v6.0
	headers = {
		'Content-Type': 'application/json',
	}
	if not isinstance(json_body, str):
		json_body = json.dumps(json_body)

	try:
		# make HTTP verb parameter case-insensitive by converting to lower()
		if verb.lower() == "get":
			resp = requests.get(es_endpoint, auth = (es_username, es_password), headers=headers, data=json_body)
		elif verb.lower() == "post":
			resp = requests.post(es_endpoint, auth = (es_username, es_password), headers=headers, data=json_body)
		elif verb.lower() == "put":
			resp = requests.put(es_endpoint, auth = (es_username, es_password), headers=headers, data=json_body)
		elif verb.lower() == "delete":
			resp = requests.delete(es_endpoint, auth = (es_username, es_password), headers=headers, data=json_body)

		# read the text object string
		try:
			resp_text = json.loads(resp.text)
		except:
			resp_text = resp.text
	except Exception as error:
		raise Exception(f'elasticsearch_curl error: {error}')
		resp_text = error

	# return the Python dict of the request
	return resp_text


def get_item_by_ds_id_elasticsearch(endpoint, username, password, ds_id):
	'''
	returns the source of the metavarset with $ds_id, false if it was not found
	'''
	response = elasticsearch_curl(f'{endpoint}metavars/_doc/{ds_id}', username, password)
	response = jsonify(response)

	if response['found']:
		return response['_source']
	else:
		raise kobaza_error.MetavarsetNotFoundError(ds_id)


def get_all_items_elasticsearch(endpoint, username, password):
	'''
	list all the ids in the es index "metavars"
	'''
	search_params = {
		"query": {
			"match_all": {}
		}
	}
	
	response = elasticsearch_curl(f'{endpoint}metavars/_search', username, password, json_body = search_params)
	
	return search_kobaza.parse_search_response(response)

def list_indices(endpoint, username, password):
	'''
	get all indices from es $endpoint
	'''
	return elasticsearch_curl(f'{endpoint}_cat/indices?v', username, password)

def confirm_es_response(endpoint, username, password, response, purpose):
	'''
	confirms whether es $response resulted in the $purpose 
	param:
		purpose: must be one of ('created', 'deleted')
	'''
	assert purpose in ['created', 'deleted']
	response = jsonify(response)
	
	if response['result'] == purpose:
		if purpose == 'created':
			try:
				item = get_item_by_ds_id_elasticsearch(endpoint, username, password, response['_id'])
				if is_valid_metavarset(item): #exists so was created
					return True
			except kobaza_error.MetavarsetNotFoundError as e: #does not exist so was not created
				return False
		elif purpose == 'deleted':
			try:
				item = get_item_by_ds_id_elasticsearch(endpoint, username, password, response['_id'])
				if is_valid_metavarset(item): #exists so was not deleted
					return False
			except kobaza_error.MetavarsetNotFoundError as e: #does not exist so was deleted
				return True
	else:
		return False

def insert_metavarset_elasticsearch(endpoint, username, password, metavar_json):
	'''
	insert $metavar_json into es $endpoint
	'''
	metavar_json = jsonify(metavar_json)

	if is_valid_metavarset(metavar_json):
		response = elasticsearch_curl(f"{endpoint}metavars/_doc/{metavar_json['ds_id']}?pretty", username, password, json_body = json.dumps(metavar_json), verb = 'put')
	else:
		raise kobaza_error.MetavarsetIsInvalid(metavar_json)
	return confirm_es_response(endpoint, username, password, response, 'created')

def delete_metavarset_elasticsearch(endpoint, username, password, ds_id):
	'''
	delete metavarset with id $ds_id from elasticsearch
	'''
	response = elasticsearch_curl(f"{endpoint}metavars/_doc/{ds_id}", username, password, verb = 'delete')
	return confirm_es_response(endpoint, username, password, response, 'deleted')


### GENERAL DATA FUNCTIONS

def is_metavarset_present(endpoint, username, password, ds):
	'''
	checks if metavarset ($ds) is present in the datastores and raises appropriate errors on inconsistancy
	params:
		$ds can be either full metavarset (type dict) or ds_id (type str)
	return:
		tuple with 2 bools for db and es being present in that order
	'''
	if is_valid_metavarset(ds):
		metavar_json = ds
		ds_id = ds['ds_id']
	elif isinstance(ds, str):
		metavar_json = {}
		ds_id = ds
	else:
		raise Exception('input is neither a string or valid metavarset')
		
	try:
		metavar_json_in_db = get_item_by_ds_id_dynamodb(ds_id)
	except kobaza_error.MetavarsetNotFoundError as e:
		metavar_json_in_db = False

	try:	
		metavar_json_in_es = get_item_by_ds_id_elasticsearch(endpoint, username, password, ds_id)
	except kobaza_error.MetavarsetNotFoundError as e:
		metavar_json_in_es = False

	db = False
	es = False

	db_inconsistant = False
	es_inconsistant = False

	if metavar_json_in_db:
		if metavar_json:
			if metavar_json_in_db == metavar_json:
				db = True
			else:
				db_inconsistant = True
		elif metavar_json_in_db == metavar_json_in_es:
			db = True
			es = True
		else:
			db_inconsistant = True
			es_inconsistant = True
	if metavar_json_in_es:
		if metavar_json:
			if metavar_json_in_es == metavar_json:
				es = True
			else:
				es_inconsistant = True
		elif metavar_json_in_db == metavar_json_in_es:
			db = True
			es = True
		else:
			db_inconsistant = True
			es_inconsistant = True

	if db_inconsistant or es_inconsistant:
		raise kobaza_error.DataStoreInconsistantError(db_inconsistant, es_inconsistant, ds_id)
	elif (not db) and (not es):
		return False
	elif db and es:
		return True
	else:
		raise kobaza_error.DataStoreInconsistantError(True, True, ds_id)


def insert_metavarset(endpoint, username, password, metavar_json):
	'''
	insert $metavar_json into both es and db, keep all data stores consistant, notify if they arent
	''' 
	metavar_json = jsonify(metavar_json)
	if not is_valid_metavarset(metavar_json): #fail if metavarset not valid
		raise kobaza_error.MetavarsetIsInvalid(metavar_json)

	#check if dataset is already present, raise appropriate errors
	try:
		present = is_metavarset_present(endpoint, username, password, metavar_json)
	except kobaza_error.DataStoreInconsistantError as e:
		raise e
	if present:
		raise kobaza_error.MetavarsetAlreadyPresentError(metavar_json['ds_id'])

	#attempt to create in db
	if insert_metavarset_dynamodb(metavar_json): #db create passed, now attempt es
		if insert_metavarset_elasticsearch(endpoint, username, password, metavar_json): #db passed, es passed
			return True
		else: #es failed. delete from db
			if delete_metavarset_dynamodb(metavar_json['ds_id']): #es failed, db passed but then successfully deleted from db, insert failed but datastores consistant, fail loudly
				raise kobaza_error.MetavarsetDatastoreOperationFailedError(metavar_json['ds_id'], 'insert')
			else: # delete on db failed, metavarset exists on db but not es, raise alarm
				raise kobaza_error.DataStoreInconsistantError(True, False)
	else: #db create failed, fail loudly
		raise kobaza_error.MetavarsetDatastoreOperationFailedError(metavar_json['ds_id'], 'insert')


def delete_metavarset(endpoint, username, password, ds_id):
	'''
	delete metavarset with $ds_id from both es and db
	'''

	#check if dataset is present, raise appropriate errors
	try:
		present = is_metavarset_present(endpoint, username, password, ds_id)
	except kobaza_error.DataStoreInconsistantError as e:
		raise e
	if not present:
		raise kobaza_error.MetavarsetNotFoundError(ds_id)

	#get the actual metavarset so as to reinsert into db if es delete failed. which is done to keep datastores consistant
	try:
		metavar_json = get_item_by_ds_id_dynamodb(ds_id)
	except kobaza_error.MetavarsetNotFoundError as e:
		raise e

	#attempt db delete
	if delete_metavarset_dynamodb(ds_id): #db delete passed, now attempt es
		if delete_metavarset_elasticsearch(endpoint, username, password, ds_id): #db passed, es passed
			return True
		else:# es failed but db passed, so put back in db
			if insert_metavarset_dynamodb(metavar_json): #es failed, db passed but then successfully put back in db, delete failed but datastores consistant, fail loudly
				raise kobaza_error.MetavarsetDatastoreOperationFailedError(ds_id, 'delete')
			else: # put back in db failed, metavarset does not exist on db but does on es, raise alarm
				raise kobaza_error.DataStoreInconsistantError(True, False)
	else: #db delete failed, fail loudly
		raise kobaza_error.MetavarsetDatastoreOperationFailedError(ds_id, 'delete')


### data upload functions

def is_allowed_file(filename: str) -> bool:
	return '.' in filename and filename.rsplit('.', 1)[-1].lower() in ALLOWED_EXTENSIONS


def get_numvars_in_uploaded_mv(metavars_upload_form:dict) -> int:
	'''
	gets the num of vars in an uploaded metavar dict
	'''
	num_var_names = sum(list(map(lambda x: x[:7] == 'varname', list(metavars_upload_form.keys()))))
	num_var_descs = sum(list(map(lambda x: x[:7] == 'vardesc', list(metavars_upload_form.keys()))))
	try:
		assert num_var_names == num_var_descs
		num_vars = num_var_names
	except AssertionError:
		raise kobaza_error.UploadedVarsNamesAndDescsCountsUnequalError(num_var_names, num_var_descs)
	return num_vars


def parse_uploaded_metavarset_form(mv_form: dict) -> dict:
	'''
	turns the form from the metavar uplaod ($mv_form) to a dict in the same format as the mv jsons
	'''
	#get num of vars, columns in data
	num_vars = get_numvars_in_uploaded_mv(mv_form)

	#get all vars and put names and descs in two lists
	var_names = []
	var_descs = []
	for i in range(1, num_vars + 1):
		var_names.append(mv_form[f'varname{i}'])
		var_descs.append(mv_form[f'vardesc{i}'])
	
	#get unique id both for the metavarset and to append to filename to make it unique
	ds_id = str(uuid.uuid4())

	#construct standard format metavarset json for uploaded metavarset
	uploaded_metavars_json = {
		'ds_id': ds_id,
		'name': mv_form['name'], 
		'ds_source': mv_form['ds_source'], 
		'last updated': mv_form['last updated'],
		'meta-attributes': {
			'context': {
				'domain': mv_form['domain'],
				'process': mv_form['process'],
				'situation': mv_form['situation']
			},
			'variables': {
				'variable_names': var_names,
				'variable_descriptions': var_descs
			},
			'size': {
				'rows': mv_form['rows'],
				'description': mv_form['description']
			}				
		}
	}

	return uploaded_metavars_json


def save_uploaded_metavarset_json():
	'''
	handle permanent storage of uploaded metavarset
	'''
	#just yeet it for now
	pass
	return


def save_uploaded_dataset_file():
	'''
	handle permanent storage of uploaded dataset
	'''
	#just yeet it for now
	pass
	return


def save_uploaded_dataset(dataset_file:werkzeug.datastructures.FileStorage):
	'''
	driver for saving entire dataset 
	'''
	dataset_filename = secure_filename(dataset_file.filename)
	save_uploaded_metavarset_json()
	save_uploaded_dataset_file()
	return










