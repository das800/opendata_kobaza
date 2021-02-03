'''
Dayan Siddiqui
2021-01-12

purpose: implement search functionality for dataset metavars
'''

import requests
import json
import os




class EsQueryHit():
	def __init__(self, id, name):
		self.id = id
		self.name = name
	
	def __repr__(self):
		return f'{self.id}\t({self.name})'





def read_creds(filename):
	'''
	reads creads from $filname assuming 1st, 2nd, 3rd lines are endpoint, u, and p respectively
	'''
	with open(filename, 'r') as creds_fh:
		endpoint = creds_fh.readline().strip()
		username = creds_fh.readline().strip()
		password = creds_fh.readline().strip()
	return endpoint, username, password




def elasticsearch_curl(es_endpoint, es_username, es_password, json_body = '', verb = 'get'):
	'''constructs curl like command from requests library'''

	# pass header option for content type if request has a body to avoid Content-Type error in Elasticsearch v6.0
	headers = {
		'Content-Type': 'application/json',
	}

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
		# catch exceptions and print errors to terminal
	except Exception as error:
		print ('\nelasticsearch_curl() error:', error)
		resp_text = error

	# return the Python dict of the request
	return resp_text


def list_indices(endpoint, username, password):
	'''get all indices from es $endpoint'''
	return elasticsearch_curl(f'{endpoint}_cat/indices?v', username, password)

def insert_metavar(endpoint, username, password, metavar_json):
	'''insert $metavar_json into es $endpoint'''
	return elasticsearch_curl(f"{endpoint}metavars/_doc/{metavar_json['ds_id']}?pretty", username, password, json_body = json.dumps(metavar_json), verb = 'put')



def parse_search_response(response):
	'''parses the elasticsearch query $response into a workable form'''
	hits = response['hits']['hits']
	
	query_results = []
	for hit in hits:
		query_results.append(EsQueryHit(hit['_source']['ds_id'], hit['_source']['name']))
	
	return query_results

def simple_search(endpoint, username, password, query):
	'''perforn simple $query over entire document for all documents at $endpoint'''
	search_params = {"query": {
			"simple_query_string": {
				"query": query
			}
		}}

	response = elasticsearch_curl(f'{endpoint}metavars/_search', username, password, json_body = json.dumps(search_params))

	return parse_search_response(response)


