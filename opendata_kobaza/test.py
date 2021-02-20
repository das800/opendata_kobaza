'''
Dayan Siddiqui
2021-02-20

purpose: automated tests for app functionality
'''

import os
import json

import kobaza_error
import data_access

#data access test
'''
test cases: 
- insert a valid data set and run validity check
- insert data set that already exists
- delete inserted set from only one of either and run validity check
- delete data set
- delete dataset that does not exist
- insert a invalid dataset and run validity check
- insert into only one of either and run validity check
- insert different datasets for same id into both and run validity check
'''

def vis_div():
	print()
	print('#' * 20)
	print()


def check_datastore_validity(endpoint, username, password):
	db_ids = set(data_access.get_all_ds_ids_dynamodb())
	es_ids = set([hit.id for hit in data_access.get_all_items_elasticsearch(endpoint, username, password)])
	
	print(db_ids == es_ids)
	ids = list(db_ids | es_ids)
	print(f"sets in store {ids}")

	for id in list(db_ids | es_ids):
		print(data_access.is_metavarset_present(endpoint, username, password, id))
	print()



def data_access_simple_insert_test(endpoint, username, password, metavar_json):
	data_access.insert_metavarset(endpoint, username, password, metavar_json)



def data_acces_simple_delete_test(endpoint,username, password, ds_id):
	data_access.delete_metavarset(endpoint, username, password, ds_id)



def data_access_test(endpoint, username, password):
	ids = data_access.get_all_ds_ids_dynamodb()
	test_id = ids[0]
	second_id = ids[1]
	db_ds = data_access.get_item_by_ds_id_dynamodb(test_id)
	second_ds = data_access.get_item_by_ds_id_dynamodb(second_id)
	assert data_access.is_valid_metavarset(db_ds)
	assert data_access.is_valid_metavarset(second_ds)

	db_ds['ds_id'] = 'test_id'
	second_ds['ds_id'] = 'test_id'


	vis_div()
	print('test 1: valid insert, success: nothing')
	### insert valid dataset test
	data_access_simple_insert_test(endpoint, username, password, db_ds)
	check_datastore_validity(endpoint, username, password)


	vis_div()
	print('test 2: insert existing, success: print already exists error')
	### insert same dataset again
	try:
		data_access_simple_insert_test(endpoint, username, password, db_ds)
	except kobaza_error.MetavarsetAlreadyPresentError as e:
		print(e)
	except Exception as e:
		raise e
	check_datastore_validity(endpoint, username, password)


	vis_div()
	print('test 3: delete only from one, success: print datastore inconsistant errors')
	### delete from one and check validity (then insert back and run validity again)
	data_access.delete_metavarset_elasticsearch(endpoint, username, password, db_ds['ds_id'])
	try:
		check_datastore_validity(endpoint, username, password)
	except kobaza_error.DataStoreInconsistantError as e:
		print(e)
	except Exception as e: 
		raise e
	data_access.insert_metavarset_elasticsearch(endpoint, username, password, db_ds)
	check_datastore_validity(endpoint, username, password)


	vis_div()
	print('test 4: simple delete, success: nothing')
	### delete dataset
	data_acces_simple_delete_test(endpoint, username, password, db_ds['ds_id'])
	check_datastore_validity(endpoint, username, password)


	vis_div()
	print('test 5: delete nonexistant, success: print not found error')
	### delete non existant dataset
	try:
		data_acces_simple_delete_test(endpoint, username, password, db_ds['ds_id'])
	except kobaza_error.MetavarsetNotFoundError as e:
		print(e)
	except Exception as e:
		raise e
	check_datastore_validity(endpoint, username, password)


	vis_div()
	print('test 6: insert invalid, success: print invalid error')	
	### insert invalid dataset test
	invalid_ds = dict(db_ds)
	del invalid_ds['meta-attributes']
	try:
		data_access_simple_insert_test(endpoint, username, password, invalid_ds)
	except kobaza_error.MetavarsetIsInvalid as e:
		print(e)
	except Exception as e:
		raise e
	check_datastore_validity(endpoint, username, password)


	vis_div()
	print('test 7: insert into one and check validity, success: print datastore inconsistant errors')
	### insert into one and check validity, (then delete and check validity again)
	data_access.insert_metavarset_dynamodb(db_ds)
	try:
		check_datastore_validity(endpoint, username, password)
	except kobaza_error.DataStoreInconsistantError as e:
		print(e)
	except Exception as e:
		raise e
	data_access.delete_metavarset_dynamodb(db_ds['ds_id'])
	check_datastore_validity(endpoint, username, password)


	vis_div()
	print('test 8: insert different sets for same id and run validity, success: print datastore inconsistant errors')
	### insert different sets into datastores, run validity and then delete from both
	data_access.insert_metavarset_dynamodb(db_ds)
	data_access.insert_metavarset_elasticsearch(endpoint, username, password, second_ds)
	try:
		check_datastore_validity(endpoint, username, password)
	except kobaza_error.DataStoreInconsistantError as e:
		print(e)
	except Exception as e:
		raise e
	data_access.delete_metavarset_dynamodb(db_ds['ds_id'])
	data_access.delete_metavarset_elasticsearch(endpoint, username, password, second_ds['ds_id'])
	check_datastore_validity(endpoint, username, password)


	

	


def main():
	endpoint, username, password = data_access.read_creds(os.path.join('.', 'security', 'kobaza_es_creds.txt'))

	data_access_test(endpoint, username, password)
	# data_access.delete_metavarset(endpoint, username, password, 'test_id')
	print('\n final validity test')
	check_datastore_validity(endpoint, username, password)

main()