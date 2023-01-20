import sys
import importlib
from os.path import dirname, basename

import argparse
parser = argparse.ArgumentParser()

parser.add_argument("--python-file", required=True, 
    help="the full path of python file that cluster should call, e.g: /path/to/python.py",
    default="")

parser.add_argument("--function", required=False, 
    help="function name need to be called",
    default="")

parser.add_argument("--arguments", required=False, nargs='+', 
    help="args to pass to the function or python file",
    default="")

args = parser.parse_args()
target = args.python_file
function = args.function
arguments = args.arguments


python_file = basename(target)
if not python_file.endswith(".py"):
    print("don't support non-python file")
    exit(1)

module = dirname(target)
parent_dir = dirname(module)

sys.path.append(module)
sys.path.append(parent_dir)

python_module_name = python_file.split(".")[0]
python_module = importlib.import_module(python_module_name)
if function and hasattr(python_module, function):
    if arguments:
        getattr(python_module, function)(*arguments)
    else:
        getattr(python_module, function)()

