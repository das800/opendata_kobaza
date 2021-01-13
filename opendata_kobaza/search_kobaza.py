'''
Dayan Siddiqui
2021-01-12

purpose: implement search functionality for dataset metavars
'''

import json
import os

# test_metavars_filepath = 'datasets/metavars/SYB58_35_Index of industrial production_clean.json'

class DsSearchRes:
    # def __init__(self, ds_id, metavar_json, query): TODO add ds_id
    def __init__(self, metavar_json, query):
        # self.ds_id = ds_id
        self.metavar_json = metavar_json
        self.query = query
        self.matches = []

    def __len__(self):
        return len(self.matches)

    def add_match(self, meta_attr_type, meta_attr):
        self.matches.append((meta_attr_type, meta_attr))
        return 0
    
    def get_best_match(self):
        #TODO just first match, make this better
        return self.matches[0]

class DsDisplaySearchRes:
    # def __init__(self, ds_id, name, best_match): TODO add ds_id
    def __init__(self, name, best_match):
        # self.ds_id = ds_id
        self.name = name
        self.best_match = best_match



def find_q_in_metavarset(metavarset_json, query):
    '''
    params
        metavarset_json: json (dict) containing a single dataset's metavars and other metadata
        query: search query to look for in dataset metavarset
    
    TODO currently uses exact match of full query, very rudimentary, make it better
    '''
    meta_attr_set = metavarset_json['meta-attributes']


    ds_sr = DsSearchRes(metavarset_json, query)

    for meta_attr_type in list(meta_attr_set.keys())[:2]:
        for meta_attr in meta_attr_set[meta_attr_type]:
            if query in meta_attr_set[meta_attr_type][meta_attr]:
                ds_sr.add_match(meta_attr_type, meta_attr)

    return ds_sr

