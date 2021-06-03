'''
Dayan Siddiqui
2021-01-12

purpose: implement search functionality for dataset metavars
'''

import requests
import json
import os

#custom imports
import data_access




class EsQueryHit():
	def __init__(self, id, name):
		self.id = id
		self.name = name
	
	def __repr__(self):
		return f'{self.id}\t({self.name})'




def parse_search_response(response):
	'''parses the elasticsearch query $response into a workable form'''
	try:
		hits = response['hits']['hits']
	except KeyError: #no results
		return []

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

	response = data_access.elasticsearch_curl(f'{endpoint}metavars/_search', username, password, json_body = json.dumps(search_params))

	return parse_search_response(response)


