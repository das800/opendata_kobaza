'''
Dayan Siddiqui
2021-05-17

purpose: automated tests for scp dataset storage
'''

import os
import json
import unittest
import sys
import subprocess

sys.path.append('../')

import kobaza_error
import data_access


server_host = 'datasets_kobaza'
server_storage_path = '/mnt/kobaza-dataset-store/data'
local_storage_path = '/home/dayan/code-bases/test'



class TestDatasetAccess(unittest.TestCase):

	def test1(self):
		'''insert a dataset not present in store (success)'''
		self.assertTrue(data_access.scp_data_storage_write('transfer-out-test.txt', server_host, server_storage_path, local_storage_path))
	
	def test2(self):
		'''delete a dataset already present in store (success)'''
		self.assertTrue(data_access.scp_data_storage_delete('transfer-out-test.txt', server_host, server_storage_path))

	def test3(self):
		'''insert a dataset already present in store (custom error)'''
		data_access.scp_data_storage_write('transfer-out-test.txt', server_host, server_storage_path, local_storage_path)
		with self.assertRaises(kobaza_error.DatasetFileAlreadyExistsError):
			data_access.scp_data_storage_write('transfer-out-test.txt', server_host, server_storage_path, local_storage_path)
		data_access.scp_data_storage_delete('transfer-out-test.txt', server_host, server_storage_path)
	
	def test4(self):
		'''insert a dataset not present in app (custom error)'''
		with self.assertRaises(kobaza_error.DatasetFileNotFoundError):
			data_access.scp_data_storage_write('doesnotexist.txt', server_host, server_storage_path, local_storage_path)
	
	def test5(self):
		'''delete a dataset not present in store (custom error)'''
		with self.assertRaises(kobaza_error.DatasetFileNotFoundError):
			data_access.scp_data_storage_delete('doesnotexist.txt', server_host, server_storage_path)

	def test6(self):
		'''read a dataset not present in app (success)'''
		self.assertTrue(data_access.scp_data_storage_read('transfer-in-test.txt', server_host, server_storage_path, local_storage_path))

	def test7(self):
		'''read a dataset already present in app (custom error)'''
		with self.assertRaises(kobaza_error.DatasetFileAlreadyExistsError):
			data_access.scp_data_storage_read('transfer-in-test.txt', server_host, server_storage_path, local_storage_path)
		subprocess.run(['rm', os.path.join(local_storage_path, 'transfer-in-test.txt')], text = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)

	def test8(self):
		'''read a dataset not present in store (custom error)'''
		with self.assertRaises(kobaza_error.DatasetFileNotFoundError):
			data_access.scp_data_storage_read('doesnotexist.txt', server_host, server_storage_path, local_storage_path)

unittest.main()