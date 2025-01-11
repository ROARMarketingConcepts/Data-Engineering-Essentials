import glob
import os
import json
import sys
import re
import pandas as pd

'''This script reads CSV files from a source directory, converts them to JSON, and writes them to a target directory'''

def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    file_path_list = re.split('[/\\\\]', file)
    table = file_path_list[-2]
    file_name = file_path_list[-1]
    columns = get_column_names(schemas, table)
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
    
def file_converter(src_base_dir, tgt_base_dir, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/*')
    if len(files) == 0:
        raise NameError(f'No files found for table "{ds_name}"')   # raise an error if no files found because we can't process empty files
    for file in files:
        df = read_csv(file, schemas)
        file_name = re.split('[/\\\\]', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)
        
def process_files(ds_names=None):
    # src_base_dir = 'data/retail_db'
    # tgt_base_dir = 'data/retail_db_json'
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    tgt_base_dir = os.environ.get('TGT_BASE_DIR')
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
    # add try-except block to catch any errors that occur during processing
        try:        
            print(f'Processing {ds_name}')
            file_converter(src_base_dir, tgt_base_dir, ds_name)
        except NameError as ne:
            print(f'Error processing {ds_name}: {ne}')   # print error message if NameError occurs  
            pass
        
if __name__ == '__main__':
    
    if len(sys.argv) == 2:
        tables = json.loads(sys.argv[1])  # provide list of tables to process   
    else:
        tables = None         # process all tables in schemas.json
        
    process_files(tables)