'''
Dayan Siddiqui
2021-01-17

purpose: provide abstraction for accessing storing the datasets and their metavars in all stores

reqs: needs aws cli to be installed and configured in the same env to work properly
'''
###with aws dynamobd (make sure aws cli is installed and configured)
import json
import os
from main import app, ALLOWED_EXTENSIONS
import requests
import boto3
import uuid
from boto3.dynamodb.conditions import Key, Attr
import werkzeug
import subprocess
import re
from typing import List
import pathlib

#custom imports
import kobaza_error
import search_kobaza

aws_db_service = 'dynamodb'
aws_region_id = 'eu-central-1'
aws_dynamodb_tablename = 'kobaza_ds_metavars'

dynamodbres = boto3.resource(aws_db_service, region_name = aws_region_id)
dynamodbclient = boto3.client(aws_db_service, region_name = aws_region_id)
table = dynamodbres.Table(aws_dynamodb_tablename)


#TODO add type hints to all functions

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
	invalidities = []
	if not isinstance(metavar_json, dict):
		invalidities.append(1)
	if not (set(metavar_json.keys()) == {'meta-attributes', 'ds_id', 'raw data file', 'ds_source', 'last updated', 'name', 'cleaned data file'}):
		invalidities.append(2)
	if not isinstance(metavar_json['meta-attributes'], dict):
		invalidities.append(3)
	if not (set(metavar_json['meta-attributes'].keys()) == {'context', 'size', 'variables'}):
		invalidities.append(4)
	if not isinstance(metavar_json['meta-attributes']['context'], dict):
		invalidities.append(5)
	if not (set(metavar_json['meta-attributes']['context'].keys()) == {'process', 'situation', 'domain'}):
		invalidities.append(6)
	if not isinstance(metavar_json['meta-attributes']['size'], dict):
		invalidities.append(7)
	if not (set(metavar_json['meta-attributes']['size'].keys()) == {'rows', 'description'}):
		invalidities.append(8)
	if not isinstance(metavar_json['meta-attributes']['variables'], dict):
		invalidities.append(9)
	if not (set(metavar_json['meta-attributes']['variables'].keys()) == {'variable_names', 'variable_descriptions'}):
		invalidities.append(10)
	if not (isinstance(metavar_json['meta-attributes']['context']['process'], str) & isinstance(metavar_json['meta-attributes']['context']['situation'], str) & isinstance(metavar_json['meta-attributes']['context']['domain'], str)):
		invalidities.append(11)
	if not (isinstance(metavar_json['meta-attributes']['size']['rows'], int) & isinstance(metavar_json['meta-attributes']['size']['description'], str)):
		invalidities.append(12)
	if not (isinstance(metavar_json['meta-attributes']['variables']['variable_names'], list) & isinstance(metavar_json['meta-attributes']['variables']['variable_descriptions'], list)):
		invalidities.append(13)
	if not (len(metavar_json['meta-attributes']['variables']['variable_names']) == len(metavar_json['meta-attributes']['variables']['variable_descriptions'])):
		invalidities.append(14)

	if invalidities:
		if verbose:
			print(invalidities)
		return False
	else:
		return True




### DYNAMODB FUNCTIONS

def get_item_by_ds_id_dynamodb(ds_id: str) -> dict:
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


def get_names_by_ds_ids_dynamodb(ds_ids:List[str]) -> dict:
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




### ELASTICSEARCH FUNCTIONS

def read_creds(filename):
	'''
	reads elasticsearch creds from $filename assuming 1st, 2nd, 3rd lines are endpoint, u, and p respectively
	reads creds and host name:
	- for elasticsearch, expects 1st, 2nd, and 3rd lines to be endpoint, username and password respectively and return in same order
	- for scp store, expects 1st, 2nd, and 3rd lines to be server hostname, server storage path, and local storage path respectively
	'''
	with open(filename, 'r') as creds_fh:
		line1 = creds_fh.readline().strip()
		line2 = creds_fh.readline().strip()
		line3 = creds_fh.readline().strip()
	return line1, line2, line3


def elasticsearch_curl(es_endpoint:str, es_username:str, es_password:str, json_body = '', verb = 'get'):
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


def get_item_by_ds_id_elasticsearch(endpoint:str, username:str, password:str, ds_id):
	'''
	returns the source of the metavarset with $ds_id, false if it was not found
	'''
	response = elasticsearch_curl(f'{endpoint}metavars/_doc/{ds_id}', username, password)
	response = jsonify(response)

	if response['found']:
		return response['_source']
	else:
		raise kobaza_error.MetavarsetNotFoundError(ds_id)


def get_all_items_elasticsearch(endpoint:str, username:str, password:str):
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

def list_indices(endpoint:str, username:str, password:str):
	'''
	get all indices from es $endpoint
	'''
	return elasticsearch_curl(f'{endpoint}_cat/indices?v', username, password)

def confirm_es_response(endpoint:str, username:str, password:str, response, purpose):
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

def insert_metavarset_elasticsearch(endpoint:str, username:str, password:str, metavar_json):
	'''
	insert $metavar_json into es $endpoint
	'''
	metavar_json = jsonify(metavar_json)

	if is_valid_metavarset(metavar_json):
		response = elasticsearch_curl(f"{endpoint}metavars/_doc/{metavar_json['ds_id']}?pretty", username, password, json_body = json.dumps(metavar_json), verb = 'put')
	else:
		raise kobaza_error.MetavarsetIsInvalid(metavar_json)
	return confirm_es_response(endpoint, username, password, response, 'created')

def delete_metavarset_elasticsearch(endpoint:str, username:str, password:str, ds_id):
	'''
	delete metavarset with id $ds_id from elasticsearch
	'''
	response = elasticsearch_curl(f"{endpoint}metavars/_doc/{ds_id}", username, password, verb = 'delete')
	return confirm_es_response(endpoint, username, password, response, 'deleted')


### GENERAL METAVARSET FUNCTIONS

def is_metavarset_present(endpoint:str, username:str, password:str, ds):
	'''
	checks if metavarset ($ds) is present in the datastores and raises appropriate errors on inconsistancy
	params:
		$ds can be either full metavarset (type dict) or ds_id (type str)
	return:
		tuple with 2 bools for db and es being present in that order
	'''
	if isinstance(ds, str):
		metavar_json = {}
		ds_id = ds
	elif is_valid_metavarset(ds):
		metavar_json = ds
		ds_id = ds['ds_id']
	else:
		raise ValueError('input is neither a string or valid metavarset')
		
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
		raise kobaza_error.DataStoreInconsistantError(db_inconsistant = db_inconsistant, es_inconsistant = es_inconsistant, suspect_id = ds_id)
	elif (not db) and (not es):
		return False
	elif db and es:
		return True
	else:
		raise kobaza_error.DataStoreInconsistantError(db_inconsistant = True, es_inconsistant = True, suspect_id = ds_id)


def insert_metavarset(endpoint:str, username:str, password:str, metavar_json) -> bool:
	'''
	insert $metavar_json into both es and db, keep all data stores consistant, notify if they arent, may raise following errors on failure (MetavarsetIsInvalid, DataStoreInconsistantError, MetavarsetAlreadyPresentError, DatastoreOperationFailedError)
	''' 
	metavar_json = jsonify(metavar_json)
	if not is_valid_metavarset(metavar_json, True): #fail if metavarset not valid
		raise kobaza_error.MetavarsetIsInvalid(metavar_json)

	#check if dataset is already present, raise appropriate errors
	try:
		present = is_metavarset_present(endpoint, username, password, metavar_json['ds_id'])
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
				raise kobaza_error.DatastoreOperationFailedError(metavar_json['ds_id'], 'insert')
			else: # delete on db failed, metavarset exists on db but not es, raise alarm
				raise kobaza_error.DataStoreInconsistantError(db_inconsistant = True, es_inconsistant = False, suspect_id = metavar_json['ds_id'])
	else: #db create failed, fail loudly
		raise kobaza_error.DatastoreOperationFailedError(metavar_json['ds_id'], 'insert')


def delete_metavarset(endpoint:str, username:str, password:str, ds_id) -> bool:
	'''
	delete metavarset with $ds_id from both es and db, may raise following errors on failure (kobaza_error.DataStoreInconsistantError, kobaza_error.MetavarsetNotFoundError, kobaza_error.MetavarsetDatastoreOperationFailedError)
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
				raise kobaza_error.DataStoreInconsistantError(db_inconsistant = True, es_inconsistant = False, suspect_id = ds_id)
	else: #db delete failed, fail loudly
		raise kobaza_error.MetavarsetDatastoreOperationFailedError(ds_id, 'delete')


### scp data storage interface function

def custom_cmd(cmd_lst:List[str], action:str, server_host:str = '') -> subprocess.CompletedProcess:
	'''
	purpose: run a shell command with params $cmd_lst, locally if $server_host is not specified, purpose of command is $action
	output: succesfully finished proc object, kobaza_error.SubprocessFailedError if process failed
	'''
	if server_host:
		finished_proc = subprocess.run(['ssh', server_host, *cmd_lst], text = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
	else:
		finished_proc = subprocess.run(cmd_lst, text = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
	if finished_proc.returncode:
		raise kobaza_error.SubprocessFailedError(finished_proc, action)
	return finished_proc


def scp_data_storage_file_exists(file_name:str, storage_path:str, server_host:str = '') -> bool:
	'''check for file existance at path. leave server blank to check local'''
	ls = custom_cmd(['ls', storage_path], server_host = server_host, action = 'check file existance')
	return file_name in ls.stdout


def scp_data_storage_validate_file_checksum(file_name:str, server_host:str, server_storage_path:str, local_storage_path:str) -> bool:
	'''
	purpose: check $file_name at both $local_storage_path and $server_host:$server_storage_path to see if their checksum matches
	output: boolean of files matching at both locations, kobaza_error.SubprocessFailedError if file does not exist at either location
	'''
	# if not scp_data_storage_file_exists(file_name, server_storage_path, server_host):
	# 	raise kobaza_error.DatasetFileNotFoundError(file_name, f'{server_host}:{server_storage_path}')
	# if not scp_data_storage_file_exists(file_name, local_storage_path):
	# 	raise kobaza_error.DatasetFileNotFoundError(file_name, local_storage_path)

	server_checksum = custom_cmd(['sha1sum', os.path.join(server_storage_path, file_name)], server_host = server_host, action = 'get sha checksum on server')
	local_checksum = custom_cmd(['sha1sum', os.path.join(local_storage_path, file_name)], action = 'get sha checksum on local')

	pattern = '^([\d\w]+) .+$'
	server_checksum = re.match(pattern, server_checksum.stdout).group(1)
	local_checksum = re.match(pattern, local_checksum.stdout).group(1)
	return server_checksum == local_checksum


def custom_scp(direction:str, file_name:str, server_host:str, server_storage_path:str, local_storage_path:str) -> bool:
	'''
	function: perform scp between server and local. direction 'in' is from server to local, 'out' is vice versa, 
	output: bool of files matching at source and destination, ValueError if direction is not valid, kobaza_error.DatasetFileNotFoundError if source file not found
	'''
	direction_vals = ['in', 'out']
	if direction not in direction_vals:
		raise ValueError(f'parameter \'direction\' must be one of {direction_vals}, instead got {direction}')

	if direction == direction_vals[0]: #in
		if scp_data_storage_file_exists(file_name, server_storage_path, server_host):
			custom_cmd(['scp', f'{server_host}:{os.path.join(server_storage_path, file_name)}', local_storage_path], action = 'scp-ing file from server to local')
		else:
			raise kobaza_error.DatasetFileNotFoundError(file_name, f'{server_host}:{server_storage_path}')
	elif direction == direction_vals[1]: #out
		if scp_data_storage_file_exists(file_name, local_storage_path):
			custom_cmd(['scp', os.path.join(local_storage_path, file_name), f'{server_host}:{server_storage_path}'], action = 'scp-ing file from local to server')
		else:
			raise kobaza_error.DatasetFileNotFoundError(file_name, local_storage_path)
	
	return scp_data_storage_validate_file_checksum(file_name, server_host, server_storage_path, local_storage_path)


def scp_data_storage_read(file_name:str, server_host:str, server_storage_path:str, local_storage_path:str) -> bool:
	'''storage read'''
	if not scp_data_storage_file_exists(file_name, server_storage_path, server_host):
		raise kobaza_error.DatasetFileNotFoundError(file_name, f'{server_host}:{server_storage_path}')

	if scp_data_storage_file_exists(file_name, local_storage_path): # file with same name exists at local location
		if scp_data_storage_validate_file_checksum(file_name, server_host, server_storage_path, local_storage_path): # file is the same
			raise kobaza_error.DatasetFileAlreadyExistsError(file_name, local_storage_path, True)
		else: #file is not the same
			return custom_scp('in', file_name, server_host, server_storage_path, local_storage_path)
	else:
		return custom_scp('in', file_name, server_host, server_storage_path, local_storage_path)



def scp_data_storage_write(file_name:str, server_host:str, server_storage_path:str, local_storage_path:str, overwrite:bool = False) -> bool:
	'''
	storage create and update, must specify overwrite to replace existing file 
	'''
	if not scp_data_storage_file_exists(file_name, local_storage_path):
		raise kobaza_error.DatasetFileNotFoundError(file_name, local_storage_path)

	if scp_data_storage_file_exists(file_name, server_storage_path, server_host): # file with same name exists at same server location
		if scp_data_storage_validate_file_checksum(file_name, server_host, server_storage_path, local_storage_path): # file is the same, fail loudly in case of unintended behavior 
			raise kobaza_error.DatasetFileAlreadyExistsError(file_name, f'{server_host}:{server_storage_path}', True)
		else: #file is not the same
			if overwrite: #have permission to overwrite
				return custom_scp('out', file_name, server_host, server_storage_path, local_storage_path)		
			else:
				raise kobaza_error.DatasetFileAlreadyExistsError(file_name, f'{server_host}:{server_storage_path}')
	else:
		return custom_scp('out', file_name, server_host, server_storage_path, local_storage_path)		


def scp_data_storage_delete(file_name:str, server_host:str, server_storage_path:str) -> bool:
	'''
	storage delete, may raise following errors on failure (DatasetFileNotFoundError)
	'''
	if not scp_data_storage_file_exists(file_name, server_storage_path, server_host):
		raise kobaza_error.DatasetFileNotFoundError(file_name, f'{server_host}:{server_storage_path}') #fail loudly if the file isnt found

	custom_cmd(['rm', os.path.join(server_storage_path, file_name)], server_host = server_host, action = 'delete dataset file on server')
	
	return not scp_data_storage_file_exists(file_name, server_storage_path, server_host)



### data upload functions

def is_allowed_file(filename: str) -> bool:
	# return '.' in filename and filename.rsplit('.', 1)[-1].lower() in ALLOWED_EXTENSIONS
	return pathlib.Path(filename).suffix in ALLOWED_EXTENSIONS


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
	ds_id = str(uuid.uuid1())

	#construct standard format metavarset json for uploaded metavarset
	uploaded_metavars_json = {
		'ds_id': ds_id,
		'name': mv_form['name'], 
		'ds_source': mv_form['ds_source'], 
		'last updated': mv_form['last_updated'], #TODO replace space with underscore (needs to be changed in database, not just here)
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
				'rows': int(mv_form['rows']),
				'description': mv_form['description']
			}				
		}
	}

	return uploaded_metavars_json


def insert_dataset_and_metavars(endpoint:str, username:str, password:str, server_host:str, server_storage_path:str, local_storage_path:str, uploaded_metavars_json:dict):
	'''
	this is the create in the crud for the dataset-metavarset pair
	'''
	dataset_filename = uploaded_metavars_json['cleaned data file']
	ds_id = uploaded_metavars_json['ds_id']

	#TODO remove this after testing the code below
	# if scp_data_storage_write(dataset_filename, server_host,server_storage_path, local_storage_path): #dataset successfully stored, now store metavarset
	# 	try: 
	# 		if insert_metavarset(endpoint, username, password, uploaded_metavars_json): #metavarset successfully stored, get rid of temp storage and done
	# 			custom_cmd(['rm', os.path.join(local_storage_path, dataset_filename)], 'deleting dataset from local storage')
	# 			return
	# 	except (kobaza_error.MetavarsetIsInvalid, kobaza_error.DataStoreInconsistantError, kobaza_error.MetavarsetAlreadyPresentError, kobaza_error.DatastoreOperationFailedError) as e: #metavarset insert failed, remove from scp
	# 		if scp_data_storage_delete(dataset_filename, server_host, server_storage_path): #dataset successfully deleted, everything consistant, raise metavarset error 
	# 			raise e
	# 		else: #dataset deletion failed, inconsistant, raise original metavarset error and another inconsistancy error
	# 			raise Exception([e, kobaza_error.DataStoreInconsistantError(scp_inconsistant = True, suspect_id = uploaded_metavars_json['ds_id'])])
	# else: #dataset failed to insert, fail loudly
	# 	raise kobaza_error.DatastoreOperationFailedError(uploaded_metavars_json['ds_id'], 'insert')

	if insert_metavarset(endpoint, username, password, uploaded_metavars_json): #metavarset successfully stored, now store dataset (delete metavarset does not return false so no else necessary)
		is_scp_stored:bool = False #flag to check successful scp deletion
		scp_failure_error = kobaza_error.DatastoreOperationFailedError(ds_id, 'insert')
		try:
			is_scp_stored = scp_data_storage_write(dataset_filename, server_host, server_storage_path, local_storage_path)
		except Exception as e:
			is_scp_stored = False
			scp_failure_error = e
		if is_scp_stored: #dataset successfully stored, return
			custom_cmd(['rm', os.path.join(local_storage_path, dataset_filename)], 'deleting dataset from local storage')
			return
		else: #dataset insertion failed, delete inserted metavarset
			try:
				delete_metavarset(endpoint, username, password, ds_id) #metavarset redeleted successfully raise the appropriate insertion failure error
			except Exception as e: #metavarset redeletion fialed, inconsistant, raise original metavarset error and another inconsistancy error
				if isinstance(e, kobaza_error.DataStoreInconsistantError): #get correct inconsistancy
					inconsistancy_error = kobaza_error.DataStoreInconsistantError(db_inconsistant = e.db_inconsistant, es_inconsistant = e.es_inconsistant, scp_inconsistant = True, suspect_id = ds_id)
				else:
					inconsistancy_error = kobaza_error.DataStoreInconsistantError(scp_inconsistant = True, suspect_id = ds_id)
				raise Exception([inconsistancy_error, scp_failure_error])
			raise scp_failure_error



# scp_data_storage_write(file_name:str, server_host:str, server_storage_path:str, local_storage_path:str, overwrite:bool = False) -> bool
# insert_metavarset(endpoint:str, username:str, password:str, metavar_json) -> bool

#TODO test this
def delete_dataset_and_metavars(endpoint:str, username:str, password:str, server_host:str, server_storage_path:str, ds_id:str):
	'''
	this is the create in the crud for the dataset-metavarset pair
	'''
	ds_metavarset = get_item_by_ds_id_dynamodb(ds_id)
	dataset_filename = ds_metavarset['cleaned data file']

	if delete_metavarset(endpoint, username, password, ds_id): #metavarset successfully deleted, now delete dataset (delete metavarset does not return false so no else necessary)
		is_scp_deleted:bool = False #flag to check successful scp deletion
		scp_failure_error = kobaza_error.DatastoreOperationFailedError(ds_id, 'delete')
		try:
			is_scp_deleted = scp_data_storage_delete(dataset_filename, server_host, server_storage_path)
		except Exception as e:
			is_scp_deleted = False
			scp_failure_error = e
		if is_scp_deleted: #dataset successfully deleted, return
			return
		else: #dataset deletion failed, reinsert metavarset
			try:
				insert_metavarset(endpoint = endpoint, username = username, password = password, metavar_json = ds_metavarset) #metavarset reinserted successfully raise the appropriate deletion failure error
			except Exception as e: #metavarset reinsertion fialed, inconsistant, raise original metavarset error and another inconsistancy error
				if isinstance(e, kobaza_error.DataStoreInconsistantError): #get correct inconsistancy
					inconsistancy_error = kobaza_error.DataStoreInconsistantError(db_inconsistant = e.db_inconsistant, es_inconsistant = e.es_inconsistant, scp_inconsistant = True, suspect_id = ds_id)
				else:
					inconsistancy_error = kobaza_error.DataStoreInconsistantError(scp_inconsistant = True, suspect_id = ds_id)
				raise Exception([inconsistancy_error, scp_failure_error])
			raise scp_failure_error



# def scp_data_storage_delete(file_name:str, server_host:str, server_storage_path:str) -> bool:
# def delete_metavarset(endpoint:str, username:str, password:str, ds_id) -> bool:






