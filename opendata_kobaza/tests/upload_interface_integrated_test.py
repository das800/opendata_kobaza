'''
Dayan Siddiqui
2021-06-12

purpose: perform intergration test on website UI
'''

import os
import sys
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import urllib.request
import urllib.error
import unittest
import json
import re


#ammend sys path and import custom scripts
sys.path.append(os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), '..'))
import data_access
from main import app, ES_CREDS_FILEPATH, SCP_STORAGE_HOST_FILEPATH


DRIVERS_PATH = '/home/dayan/drivers/browser-drivers'
CHROME_DRIVER = 'chromedriver'

WEBSITE_ADDRESS = 'http://localhost:5000'

UL_TEST_DATA_PATH = os.path.join(app.root_path, 'tests')
UL_TEST_METAVARS_FILENAME = 'ul_test_dataset_raspicputemp_metavars.json'
UL_TEST_DATASET_FILENAME = 'ul_test_dataset_raspicputemp_rawdata.csv'

#this mapes the ids of the upload form inputs to their place in the metavarset_json. THEY INPUT ELEMENTS IDS ARE HARDCODED AND MUST BE CHANGED IF THE INPUT IDS IN THE UPLOAD FORM CHANGE. AND THE LAST TWO KEYS MUST BE THE IDS FOR VARIABLE NAME AND VARIABLE DESCRIPTION SANS NUMBER
FORM_INPUT_MV_KEY_MAP = {
	'name': ['name'],
	'ds_source': ['ds_source'],
	'last_updated': ['last updated'],
	'domain': ['meta-attributes', 'context', 'domain'],
	'process': ['meta-attributes', 'context', 'process'],
	'situation': ['meta-attributes', 'context', 'situation'],
	'rows': ['meta-attributes', 'size', 'rows'],
	'description': ['meta-attributes', 'size', 'description'],
	'varname': ['meta-attributes', 'variables', 'variable_names'],
	'vardesc': ['meta-attributes', 'variables', 'variable_descriptions']
}

def get_mv_attr_from_input_id(input_id: str, mv_json_dict: dict, id_key_map: dict = FORM_INPUT_MV_KEY_MAP) -> str:
	is_var = False
	varname_match = re.match('^(varname)(\d+)', input_id)
	vardesc_match = re.match('^(vardesc)(\d+)', input_id)

	get_groups = lambda x: (x.group(1), int(x.group(2)) - 1)

	if varname_match:
		is_var = True
		input_id, varind = get_groups(varname_match)
		# input_id = varname_match.group(1)
		# varind = int(varname_match.group(2))
	elif vardesc_match:
		is_var = True
		input_id, varind = get_groups(vardesc_match)
		# input_id = vardesc_match.group(1)
		# varind = int(vardesc_match.group(2))

	#get list of keys to access the field in mv
	mv_key_ls = id_key_map[input_id]

	output = mv_json_dict
	for key in mv_key_ls:
		output = output[key]

	# for vars the output is the list of all vars so access the right index for the var
	if is_var:
		output = output[varind]

	return output


class WebInterfaceTest(unittest.TestCase):
	test_mv_json = {}	
	uploaded_mv_json = {}

	def setUp(self):
		self.driver = webdriver.Chrome(os.path.join(DRIVERS_PATH, CHROME_DRIVER))


	#TODO test uplaod dataset functionality (delete it in the upload itself)
	def test_access_upload_page(self):
		test_driver = self.driver
		
		test_driver.get(WEBSITE_ADDRESS)

		#go to first dataset page link
		upload_page_url = test_driver.find_element_by_id("upload_button").get_attribute("href")
		
		#try to open
		url = upload_page_url
		try:
			urllib.request.urlopen(url)
		except urllib.error.HTTPError:
			self.fail('HTTP error thrown')


	def test_upload_dataset(self):
		with open(os.path.join(UL_TEST_DATA_PATH, UL_TEST_METAVARS_FILENAME), 'r') as fh:
			mv_json_dict = json.load(fh)

		self.assertTrue(len(mv_json_dict['meta-attributes']['variables']['variable_names']) == len(mv_json_dict['meta-attributes']['variables']['variable_descriptions']), 'ValueError: input metavars variable names and variable descriptions have different counts')
		num_of_vars = len(mv_json_dict['meta-attributes']['variables']['variable_names'])

		test_driver = self.driver
		
		test_driver.get(WEBSITE_ADDRESS)

		#get upload page link and append parameter for number of variables TODO change this so as to use the "add variable button so it gets tested too"
		upload_page_link = test_driver.find_element_by_id("upload_button").get_attribute("href")
		upload_page_link = f'{upload_page_link}{num_of_vars}'
		test_driver.get(upload_page_link)

		upload_form = test_driver.find_element_by_id('dataset_ul_form')

		#get input ids and extend var ids by number of vars
		input_elems_ids_ls = list(FORM_INPUT_MV_KEY_MAP.keys())
		input_elems_ids_ls = input_elems_ids_ls[:-2] + [id + str(num + 1) for num in range(num_of_vars) for id in input_elems_ids_ls[-2:]]

		#render xpath using id names ls
		input_elems_xpath = './*[' + ' or '.join(['@id=\'' + id + '\'' for id in input_elems_ids_ls]) + ']'

		#this line combines the last two and is unused because its unreadable but left in becuase its gnarly
		# input_elems_xpath = './*[' + ' or '.join(['@id=\'' + id + '\'' for id in input_elems_ids_ls[:-2] + [id + str(num + 1) for num in range(num_of_vars) for id in input_elems_ids_ls[-2:]]]) + ']'

		#find non-file input elements by xpath and input them from metavarset
		for input_elem in upload_form.find_elements_by_xpath(input_elems_xpath):
			input_id = input_elem.get_attribute('id')
			input_elem.clear()
			if input_elem.get_attribute('type') == 'date': #browser may read date in mm/dd/yy format or dd/mm/yy format so just set the value in the yyyy-mm-dd format that html uses in the backend
				test_driver.execute_script("arguments[0].setAttribute('value',arguments[1])", input_elem, get_mv_attr_from_input_id(input_id, mv_json_dict))
			else: #other inputs just get entered
				input_elem.send_keys(get_mv_attr_from_input_id(input_id, mv_json_dict))

		#handle file input after all metavar inputs
		file_input_elem = upload_form.find_element_by_xpath('./input[@type=\'file\']')
		file_input_elem.clear()
		file_input_elem.send_keys(os.path.join(UL_TEST_DATA_PATH, UL_TEST_DATASET_FILENAME))

		# submit form
		upload_form.find_element_by_xpath('./input[@type=\'submit\']').click()

		#get creds to be able to delete the uploaded dataset
		es_endpoint, es_username, es_password = data_access.read_creds(ES_CREDS_FILEPATH)
		scp_hostname, scp_server_path, _ = data_access.read_creds(SCP_STORAGE_HOST_FILEPATH)

		#read the submitted metavarset json from the upload confirmation page and then delete the dataset and metavarset
		try:
			uled_mv_str = test_driver.find_element_by_id('uled_mv').text
			uled_mv_json = json.loads(uled_mv_str)
			data_access.delete_dataset_and_metavars(es_endpoint, es_username, es_password, scp_hostname, scp_server_path, uled_mv_json['ds_id'])
		except selenium.common.exceptions.NoSuchElementException:
			self.fail('no uploaded metavar json present on upload confirmation page, probably HTTP error thrown (cant tell with selenium)')

		del uled_mv_json['ds_id']
		del uled_mv_json['raw data file']
		del uled_mv_json['cleaned data file']

		self.__class__.test_mv_json = mv_json_dict
		self.__class__.uploaded_mv_json = uled_mv_json


	def test_validate_uploaded_dataset(self):
		mv_json = self.__class__.test_mv_json
		uled_mv_json = self.__class__.uploaded_mv_json

		self.assertTrue(mv_json == uled_mv_json, 'Metavarset to upload, does not match uploaded Metavarset recieved back from upload confirmation page')

		


	def tearDown(self):
		self.driver.close()


unittest.main()