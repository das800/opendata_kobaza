'''
Dayan Siddiqui
2021-02-16

purpose: implement custom errors
'''

class DataStoreInconsistantError(Exception):
	'''
	custom error to store how the datastores are inconsistant, which are ok and which are not
	'''
	def __init__(self, db_inconsistant, es_inconsistant, suspect_id = ''): #TODO add ds_id or list of that cause inconsistancy
		self.db_inconsistant = bool(db_inconsistant)
		self.es_inconsistant = bool(es_inconsistant)
		self.suspect_id = suspect_id
		self.message = f"compared to before current operation, db is {'inconsistant' if self.db_inconsistant else 'consistant'}, and es is {'inconsistant' if self.es_inconsistant else 'consistant'}{'; suspect id(s) is/are ' + str(self.suspect_id) if self.suspect_id else ''}.{' FIX URGENTLY' if self.db_inconsistant or self.es_inconsistant else ''}"
		super().__init__('Datastores are inconsistant')
	
	def __str__(self):
		return self.message


class MetavarsetIsInvalid(Exception):
	'''
	error to signify that metavarset is invalid
	'''
	def __init__(self, metavarset_json):
		self.metavarset_json = metavarset_json
		super().__init__(f"following object is not a valid metavarset json:\n{self.metavarset_json}\n")

class MetavarsetAlreadyPresentError(Exception):
	'''
	custom error for insert to communicate that the metavarset is already present in the data store
	'''
	def __init__(self, ds_id):
		self.ds_id = ds_id
		super().__init__(f'metavarset with id {self.ds_id} already exists in datastore')

class MetavarsetNotFoundError(Exception):
	'''
	custom error if metavarset does not exist in datastores
	'''
	def __init__(self, ds_id):
		self.ds_id = ds_id
		super().__init__(f'metavarset with id {self.ds_id} does not exist in datastore')


class MetavarsetDatastoreOperationFailedError(Exception):
	'''
	custom error if metavarset does not exist in datastores
	'''
	def __init__(self, ds_id, operation):
		'''
		$operation can be either 'insert' or 'delete'
		'''
		self.ds_id = ds_id
		self.operation = operation
		super().__init__(f'datastore {operation} operation on metavarset {ds_id} fialed')


class UploadedVarsNamesAndDescsCountsUnequalError(Exception):
	'''
	custom error for when the number of var names and var descs in an uploaded metavar set do not match
	'''
	def __init__(self, count_var_names: int, count_var_descs: int):
		self.count_var_names = count_var_names
		self.count_var_descs = count_var_descs
		super().__init__('var name and desc counts are unequal')

	def __str__(self):
		return f"number of var names ({self.count_var_names}) does not match number of var descs ({self.count_var_descs})"


class FileNotFoundInMetavarsetUploadError(Exception):
	'''
	custom error for when the metavarset upload request object does not contain the raw data file
	'''
	def __init__(self, file_input_name:str):
		self.file_input_name = file_input_name
		super().__init__(f"could not find key \'{file_input_name}\' in metavarset upload object")