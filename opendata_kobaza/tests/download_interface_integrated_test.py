'''
Dayan Siddiqui
2021-06-12

purpose: perform intergration test on website UI
'''

import os
import io
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import csv
import urllib.request
import urllib.error
import unittest
import re



DRIVERS_PATH = '/home/dayan/drivers/browser-drivers'
CHROME_DRIVER = 'chromedriver'

# WEBSITE_ADDRESS = 'http://localhost:5000'
WEBSITE_ADDRESS = 'http://kobaza.com'

class WebInterfaceTest(unittest.TestCase):
	csv_head = ''

	def setUp(self):
		self.driver = webdriver.Chrome(os.path.join(DRIVERS_PATH, CHROME_DRIVER))


	def test_access_homepage(self):
		url = WEBSITE_ADDRESS
		try:
			urllib.request.urlopen(url)
		except urllib.error.HTTPError:
			self.fail('HTTP error thrown')


	def test_access_dataset_page(self):
		test_driver = self.driver
		
		test_driver.get(WEBSITE_ADDRESS)

		#go to first dataset page link
		dataset_page_url = test_driver.find_element_by_name("dataset").find_element_by_tag_name('a').get_attribute("href")
		
		#try to open
		url = dataset_page_url
		try:
			urllib.request.urlopen(url)
		except urllib.error.HTTPError:
			self.fail('HTTP error thrown')


	def test_download_dataset(self): #TODO figure out why this throws a socket not closed warning and fix it. might be mem leak (in python?)
		test_driver = self.driver
		
		test_driver.get(WEBSITE_ADDRESS)

		#go to first dataset page link
		dataset_page_link = test_driver.find_element_by_name("dataset").find_element_by_tag_name('a') 
		dataset_page_link.click()

		#get link to download dataset
		dataset_dl_url = test_driver.find_element_by_id('dl_file').get_attribute("href")

		#try to open
		url = dataset_dl_url
		try:
			with urllib.request.urlopen(url) as dataset_dl_response: #TODO figure out why this raises a socket not closed warning. the response header says to close it and i use a context manager to close it here so why?
				#get encoding and read begining of dataset csv as string io
				enc = re.search('charset=([\w-]+)', dataset_dl_response.getheader('Content-Type')).group(1)
				dataset_head_iostream = io.StringIO(dataset_dl_response.read(1024).decode(enc))
				self.__class__.csv_head = dataset_head_iostream.read()

		except urllib.error.HTTPError:
			self.fail('HTTP error thrown')


	def test_validate_downloaded_dataset(self):
		#get first line of csv
		head_line = self.__class__.csv_head

		#try to infer dialect
		try:
			dialect = csv.Sniffer().sniff(head_line) #TODO put this test in the upload as well
		except:
			self.fail('CSV dialect not inferrable, might not be CSV')

		#check if file is a dos/unix csv/tsv
		self.assertTrue((dialect.delimiter in [',', '\t']) and (dialect.lineterminator in ['\r\n', '\n']))


		


	def tearDown(self):
		self.driver.close()


unittest.main()