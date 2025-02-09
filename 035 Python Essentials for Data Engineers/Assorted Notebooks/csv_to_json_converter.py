import glob
import pandas as pd
import numpy as np
import json
import re
import os

def get_column_names(schemas,ds_name,sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details,key=lambda col:col[sorting_key])   
    return [col['column_name'] for col in columns]  

def read_csv(file, schemas):
    file_path_list = re.split('[/]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
        )
    
def file_converter(ds_name, src_base_dir, tgt_base_dir):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/*')

    for file in files:
        print(f'Processing {file}')
        df = read_csv(file, schemas)
        file_name = re.split('[/]', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)
        
#  Variables

src_base_dir = '/Users/woodzsan/Desktop/Machine Learning and Data Analysis/Data_Engineering_Essentials_using_SQL_Python_and_PySpark/data/retail_db'
tgt_base_dir = '/Users/woodzsan/Desktop/Machine Learning and Data Analysis/Data_Engineering_Essentials_using_SQL_Python_and_PySpark/data/retail_db_json'
tables = ['categories', 'customers', 'departments', 'order_items', 'orders', 'products']

#  Main program

for table in tables:
    file_converter(table, src_base_dir, tgt_base_dir)

