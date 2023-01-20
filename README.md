# databricks_extension

## databricks usage

list all the *interactive cluster* (job cluster not included) usage, break down by cluster and/or user.

example:

```python /home/gavin/cluster_usage/clusters_usage_stat.py --send_email true --report_fmt=html```

you need a environment file `.env` in the same folder with value like:

```
host = https://xxxx.azuredatabricks.net
token = dapiaxxxx
```
## pyspark caller

by default databricks job api (https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/2.0/jobs?source=recommendations#--sparksubmittask) in python allows you to specify a `python_file` parameter, but by default the cluster will copy the file to `/databricks/driver/` folder, while leave rest of your code remain in origin location, that will lead to problems such as "module not found" error.

The idea is use a "wrapper" python file in `/databricks/driver/` to call your actual python code located on other path (e.g: `/mnt/artifacts/app1/main.py`) 

example:

```
'spark_python_task': {
    'python_file':'dbfs:/mnt/artifacts/app_common/pyspark_caller.py',
    'parameters':['--python-file','/dbfs/mnt/artifacts/app1/main.py']
}
```