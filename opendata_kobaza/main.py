'''
Dayan Siddiqui
2020-12-29

purpose: test out templating in flask by serving the meta vars of the 2 processed datasets
'''

# import general modules
import json
import os
import uuid
from flask import Flask, render_template, send_from_directory, request, redirect, url_for, flash
from werkzeug.utils import secure_filename

app = Flask(__name__)

# import my modules
import search_kobaza
import data_access

#softcode paths globally
# DS_FOLDER = os.path.join(app.root_path, 'datasets', 'sets')
DS_FOLDER = data_access.DS_FOLDER
ES_CREDS_FILEPATH = os.path.join(app.root_path, 'security', 'kobaza_es_creds.txt')

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
def dl_dataset(dataset_filename):
	return send_from_directory(DS_FOLDER, dataset_filename)


@app.route('/dataset_search', methods = ['GET', 'POST'])
def search_ds():
	endpoint, username, password = data_access.read_creds(ES_CREDS_FILEPATH)
	query = request.form['searchbar']

	results = search_kobaza.simple_search(endpoint, username, password, query)

	return render_template('search_results.html', query = query, results = results)


@app.route('/upload_dataset/', methods = ['GET', 'POST'])
@app.route('/upload_dataset/<int:vars>', methods = ['GET', 'POST'])
def upload_dataset(vars:int = 1, allowed_filetypes = data_access.ALLOWED_EXTENSIONS):
	prefilled_metavars = {}
	if request.method == 'POST':
		prefilled_metavars = dict(request.form)
		# vars = data_access.get_numvars_in_uploaded_mv(prefilled_metavars) + 1
	return render_template('metavar_upload.html', vars = vars, prefilled_metavars = prefilled_metavars, allowed_filetypes = allowed_filetypes)


@app.route('/dataset_uploaded', methods = ['POST'])
def handle_dataset_upload():
	file_input_form_name = 'raw_file'

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
			
		# checks passed now process file TODO process file
		dataset_filename = secure_filename(dataset_file.filename)
		print(dataset_filename)
		print(dataset_file)
		print(type(dataset_file))
	# 	dataset_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
	# 	return redirect(url_for('uploaded_file', filename=filename))

	return render_template('test.html', output = "upload feature back-end has not been implemented yet. no data has been uploaded")


#misc
@app.route('/favicon.ico')
def favicon():
	return send_from_directory(os.path.join(app.root_path, 'static', 'img'), 'favicon.ico', mimetype = 'image/vnd.microsoft.icon')






#the nginx server proxy means that these server variables are fine for gunicorn
if __name__ == '__main__':
   app.run(host = '0.0.0.0', port = 5000)
