'''
Dayan Siddiqui
2020-12-29

purpose: test out templating in flask by serving the meta vars of the 2 processed datasets
'''

import json
import os
from flask import Flask, render_template, send_from_directory
app = Flask(__name__)

ds_metavars_folder = os.path.join(app.root_path, 'metavars')

ds_metavars_filenames = {
    'pop': 'SYB63_1_202009_Population, Surface Area and Density_clean.json', 
    'ind': 'SYB58_35_Index of industrial production_clean.json'
    }

@app.route('/')
def main():
    return f"choose one of {ds_metavars_filenames.keys()}"

@app.route('/<dataset>')
def dataset_page(dataset):
    with open(os.path.join(ds_metavars_folder, ds_metavars_filenames[dataset])) as json_fh:
        ds_metavars = json.load(json_fh)
    
    return render_template('dataset_page.html', meta = ds_metavars)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico',mimetype='image/vnd.microsoft.icon')

if __name__ == '__main__':
   app.run(host = '0.0.0.0', port = 80)
