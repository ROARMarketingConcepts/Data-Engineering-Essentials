# %%
import glob
import os
import json
import re
import pandas as pd

# %%
def get_column_names(schemas, table, sorting_key='column_position'):
    column_details = schemas[table]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

# %%
def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    table = file_path_list[-2]
    file_name = file_path_list[-1]
    columns = get_column_names(schemas, table)
    df = pd.read_csv(file, names=columns)
    return df

# %%
def to_json(df, tgt_base_dir, table, file_name):
    json_file_path = f'{tgt_base_dir}/{table}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{table}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
    )

# %%
def file_converter(src_base_dir, tgt_base_dir, table):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{table}/part-*')

    for file in files:
        df = read_csv(file, schemas)
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df, tgt_base_dir, table, file_name)

# %%
def process_files(tables=None):
    src_base_dir = '/Users/woodzsan/Desktop/Machine Learning and Data Analysis/Data-Engineering-Essentials/035 Python Essentials for Data Engineers/05 Project 1 - File Format Converter/data/retail_db'
    tgt_base_dir = '/Users/woodzsan/Desktop/Machine Learning and Data Analysis/Data-Engineering-Essentials/035 Python Essentials for Data Engineers/05 Project 1 - File Format Converter/data/retail_db_json'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not tables:
        tables = schemas.keys()
    for table in tables:
        print(f'Processing {table}')
        file_converter(src_base_dir, tgt_base_dir, table)

# %%
process_files()

# %%
schemas = json.load(open('/Users/woodzsan/Desktop/Machine Learning and Data Analysis/Data-Engineering-Essentials/035 Python Essentials for Data Engineers/05 Project 1 - File Format Converter/data/retail_db/schemas.json'))

# %%
schemas.keys()

# %%



