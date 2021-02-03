'''
Dayan Siddiqui
2020-12-29

purpose: test out templating in flask by serving the meta vars of the 2 processed datasets
'''

# import general modules
import json
import os
from flask import Flask, render_template, send_from_directory, request
app = Flask(__name__)

# import my modules
import search_kobaza
import data_access

#softcode paths globally
ds_folder = os.path.join(app.root_path, 'datasets', 'sets')
es_creds_filepath = os.path.join(app.root_path, 'security', 'kobaza_es_creds.txt')


#main function
@app.route('/')
def main():
	ds_names_dict = data_access.get_names_by_ds_ids(data_access.get_all_ds_ids()[:10])
	return render_template('home.html', ds_names_dict = ds_names_dict)


#dataset functions
@app.route('/datasets/<ds_id>')
def dataset_page(ds_id):
	ds_metavars = data_access.get_item_by_ds_id(ds_id)
	return render_template('dataset_page.html', metavars_dict = ds_metavars)


@app.route('/dl_dataset/<dataset_filename>')
def dl_dataset(dataset_filename):
	return send_from_directory(ds_folder, dataset_filename)


@app.route('/dataset_search', methods = ['GET', 'POST'])
def search_ds():
	endpoint, username, password = search_kobaza.read_creds(es_creds_filepath)
	query = request.form['searchbar']

	results = search_kobaza.simple_search(endpoint, username, password, query)

	return render_template('search_results.html', query = query, results = results)


#misc
@app.route('/favicon.ico')
def favicon():
	return send_from_directory(os.path.join(app.root_path, 'static', 'img'), 'favicon.ico',mimetype='image/vnd.microsoft.icon')






#the nginx server proxy means that these server variables are fine for gunicorn
if __name__ == '__main__':
   app.run(host = '0.0.0.0', port = 5000)
