'''
Dayan Siddiqui
2020-12-29

purpose: test out templating in flask by serving the meta vars of the 2 processed datasets
'''

# import general modules
import json
import os
import uuid
from flask import Flask, render_template, send_from_directory, request, redirect, url_for, flash, after_this_request
from werkzeug.utils import secure_filename
import unicodedata
import re

app = Flask(__name__)

#softcode paths globally
DS_FOLDER = os.path.join(app.root_path, 'datasets', 'sets')
ES_CREDS_FILEPATH = os.path.join(app.root_path, 'security', 'kobaza_es_creds.txt')
SCP_STORAGE_HOST_FILEPATH = os.path.join(app.root_path, 'security', 'kobaza_scp_host.txt')
ALLOWED_EXTENSIONS = ['.csv', '.tsv']

# import my modules after globals to prevent circular imports
import search_kobaza
import data_access
import kobaza_error

app.config['UPLOAD_FOLDER'] = DS_FOLDER
app.secret_key = str(uuid.uuid4())




#main function
@app.route('/')
def main():
	ds_names_dict = data_access.get_names_by_ds_ids_dynamodb(data_access.get_all_ds_ids_dynamodb()[:10])
	return render_template('home.html', ds_names_dict = ds_names_dict)


#dataset functions
@app.route('/datasets/<ds_id>')
def dataset_page(ds_id):
	ds_metavars = data_access.get_item_by_ds_id_dynamodb(ds_id)
	return render_template('dataset_page.html', metavars_dict = ds_metavars)


@app.route('/dl_dataset/<dataset_filename>')
def dl_dataset(dataset_filename): #TODO make this get data set from scp storage
	server_host, server_storage_path, _ = data_access.read_creds(SCP_STORAGE_HOST_FILEPATH)
	if data_access.scp_data_storage_read(dataset_filename, server_host, server_storage_path, DS_FOLDER):
		@after_this_request
		def remove_file(response):
			try:
				os.remove(os.path.join(DS_FOLDER, dataset_filename))
			except Exception as e:
				app.logger.error("Error removing or closing downloaded file handle", e)
				raise e
			return response
		return send_from_directory(DS_FOLDER, dataset_filename)
	else:
		return "download fialed" #TODO real error 


@app.route('/dataset_search', methods = ['GET', 'POST'])
def search_ds():
	endpoint, username, password = data_access.read_creds(ES_CREDS_FILEPATH)
	query = request.form['searchbar']

	results = search_kobaza.simple_search(endpoint, username, password, query)

	return render_template('search_results.html', query = query, results = results)


@app.route('/upload_dataset/', methods = ['GET', 'POST'])
@app.route('/upload_dataset/<int:vars>', methods = ['GET', 'POST'])
def upload_dataset(vars:int = 1, allowed_filetypes = ALLOWED_EXTENSIONS):
	prefilled_metavars = {}
	if request.method == 'POST':
		prefilled_metavars = dict(request.form)
		# vars = data_access.get_numvars_in_uploaded_mv(prefilled_metavars) + 1
	return render_template('metavar_upload.html', vars = vars, prefilled_metavars = prefilled_metavars, allowed_filetypes = allowed_filetypes)



#to aid the uplaod function in cleaning filename
def get_valid_filename(name):
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'
    """
    s = str(name).strip().replace(' ', '_')
    s = re.sub(r'(?u)[^-\w.]', '', s)
    if s in {'', '.', '..'}:
        raise ValueError(f"Could not derive file name from {s}")
    return s

@app.route('/dataset_uploaded', methods = ['POST'])
def handle_dataset_upload():
	file_input_form_name = 'raw_file'
	es_endpoint, es_username, es_password = data_access.read_creds(ES_CREDS_FILEPATH)
	scp_hostname, scp_server_path, _ = data_access.read_creds(SCP_STORAGE_HOST_FILEPATH)
	scp_local_path = DS_FOLDER

	if request.method == 'POST':
		#handle metavarset
		form_metavars = dict(request.form)
		uploaded_metavars_json = data_access.parse_uploaded_metavarset_form(form_metavars)

		#handle dataset file
		try:
			dataset_file = request.files[file_input_form_name]
		except KeyError:
			raise kobaza_error.FileNotFoundInMetavarsetUploadError(file_input_form_name)
		# if user does not select file, browser can also submit an empty part without filename
		if (not dataset_file.filename) or (not dataset_file):
			flash('No file selected. Please select dataset file to upload')
			return redirect(url_for('upload_dataset', vars = data_access.get_numvars_in_uploaded_mv(form_metavars)), code = 307)
		if not data_access.is_allowed_file(dataset_file.filename):
			flash('Wrong filetype selected. Please upload a valid filetype')
			return redirect(url_for('upload_dataset', vars = data_access.get_numvars_in_uploaded_mv(form_metavars)), code = 307)
			
		#create dataset filename by using ds id and uploaded files name
		dataset_filename = secure_filename(dataset_file.filename)
		dataset_filename = get_valid_filename(dataset_filename)
		dataset_filename = f"{uploaded_metavars_json['ds_id']}__{dataset_filename}" #prepend id to make filename unique
		uploaded_metavars_json['raw data file'] = dataset_filename
		uploaded_metavars_json['cleaned data file'] = dataset_filename

		#upload dataset first
		dataset_file.save(os.path.join(scp_local_path, dataset_filename))

		data_access.insert_dataset_and_metavars(es_endpoint, es_username, es_password, scp_hostname, scp_server_path, scp_local_path, dataset_filename, uploaded_metavars_json)
		return render_template('test.html', output = f"Your dataset {uploaded_metavars_json['name']} has been uploaded (id: {uploaded_metavars_json['ds_id']})") #TODO make another upload confirmation page



#misc
@app.route('/favicon.ico')
def favicon():
	return send_from_directory(os.path.join(app.root_path, 'static', 'img'), 'favicon.ico', mimetype = 'image/vnd.microsoft.icon')






#the nginx server proxy means that these server variables are fine for gunicorn
if __name__ == '__main__':
   app.run(host = '0.0.0.0', port = 5000)
