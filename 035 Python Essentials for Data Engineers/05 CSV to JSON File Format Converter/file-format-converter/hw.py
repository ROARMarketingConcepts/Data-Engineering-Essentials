import sys
import os
import json

args = sys.argv
arg = args[1]
prog_file = args[0]
print(f'Arguments: {args}')
print(f'Program file: {prog_file}')
print(f'Argument: {arg}')

print(json.loads(arg))



# appdev -> appdbdev(retail_db,retail_user, retaildevpassword)
# appuat -> appdbuat(retail_db,retail_user, retailuatpassword)$