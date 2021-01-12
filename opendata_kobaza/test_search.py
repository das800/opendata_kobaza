'''
Dayan Siddiqui
2021-01-12

purpose: implement search functionality for dataset metavars
'''

import json
import os

test_metavars_filepath = 'datasets/metavars/SYB58_35_Index of industrial production_clean.json'

def find_q_in_metavarset(metavarset_json, query):
    '''
    params
        metavarset_json: json (dict) containing a single dataset's metavars and other metadata
        query: search query to look for in dataset metavarset
    
    currently uses exact match of full query, very rudimentary
    '''
    meta_attr_set = metavarset_json['meta-attributes']

    matches = []

    # print(list(meta_attr_set.keys())[:2])

    for meta_attr_type in list(meta_attr_set.keys())[:2]:
        # if meta_attr_type != 'size': #loop through
        for meta_attr in meta_attr_set[meta_attr_type]:
            if query in meta_attr_set[meta_attr_type][meta_attr]:
                matches.append((meta_attr_type, meta_attr))
        # else: #cant loop through size, take size description
        #     if query in meta_attr_set[meta_attr_type][1]:
        #         matches.append()
    return matches





with open (test_metavars_filepath, 'r') as json_fh:
    print(find_q_in_metavarset(json.load(json_fh), 'zinc'))