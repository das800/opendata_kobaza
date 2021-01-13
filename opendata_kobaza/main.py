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

#softcode paths globally
ds_metavars_folder = os.path.join(app.root_path, 'datasets', 'metavars')
ds_folder = os.path.join(app.root_path, 'datasets', 'sets')

ds_metavars_filenames = {
    'pop': 'SYB63_1_202009_Population, Surface Area and Density_clean.json', 
    'ind': 'SYB58_35_Index of industrial production_clean.json'
    }


#main function
@app.route('/')
def main():
    ds_metavars_names = {ds_id: ds_metavars_filenames[ds_id][:-11] for ds_id in ds_metavars_filenames}
    return render_template('home.html', ds_names_dict = ds_metavars_names)


#dataset functions
@app.route('/datasets/<ds_id>')
def dataset_page(ds_id):
    with open(os.path.join(ds_metavars_folder, ds_metavars_filenames[ds_id])) as json_fh:
        ds_metavars = json.load(json_fh)
    
    return render_template('dataset_page.html', meta = ds_metavars)


@app.route('/dl_dataset/<dataset_filename>')
def dl_dataset(dataset_filename):
    return send_from_directory(ds_folder, dataset_filename)


@app.route('/dataset_search', methods = ['GET', 'POST'])
def search_ds():
    query = request.form['searchbar']
    results = []
    for ds_id in ds_metavars_filenames:
        with open(os.path.join(ds_metavars_folder, ds_metavars_filenames[ds_id])) as json_fh:
            ds_sr = search_kobaza.find_q_in_metavarset(json.load(json_fh), query)
            if len(ds_sr):
                ds_sr_best_match = ds_sr.get_best_match()
                results.append((ds_id, search_kobaza.DsDisplaySearchRes(ds_sr.metavar_json['name'], ds_sr.metavar_json['meta-attributes'][ds_sr_best_match[0]][ds_sr_best_match[1]]))) # TODO make ds_id a part of the display sr object putting it with the object as a tuple ad hoc is very bad

    return render_template('search_results.html', query = query, results = results)


#misc
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static', 'img'), 'favicon.ico',mimetype='image/vnd.microsoft.icon')






#the nginx server proxy means that these server variables are fine for gunicorn
if __name__ == '__main__':
   app.run(host = '0.0.0.0', port = 5000)
