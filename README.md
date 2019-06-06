

# IBM Cloud Functions on Apache Airflow

This repository contains an Apache Airflow Plugin that provides new operators to easily deploy serverless functions tasks on IBM Cloud. It uses the IBM-Cloud PyWren library, which offers the possibility to invoke thousands of parallel tasks that run simultaneously, focusing on tasks related to big data analytics. Using Airflow default operators along with this plugin's, workflows involving decisions based on big data analytics results can be easily developed.
- Apache Airflow: https://github.com/apache/airflow
- PyWren IBM Cloud: https://github.com/pywren/pywren-ibm-cloud
- CloudButton Project: http://cloudbutton.eu/


## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Initial requirements
In order to execute functions on IBM Cloud using PyWren, the following requirements are needed:

- An IBM Cloud Functions [account](https://cloud.ibm.com/openwhisk/). 
- An IBM Cloud Object Storage [account](https://www.ibm.com/cloud/object-storage).
- Python 3.5 or newer.

### Installing Apache Airflow

Use `pip` to install the last stable version of Apache Airflow. Make sure to install the Python3 version:

```
pip3 install apache-airflow
```

### Installing IBM Cloud Functions Plugin

Move to Airflow home directory. The default location is `~/airflow`:

`cd ~/airflow`

Create the `plugins` directory, and cd into it:

`mkdir plugins`
`cd plugins`

Clone the plugin repository into it:

`git clone https://github.com/aitorarjona/ibm_cloud_functions_airflow_plugin.git`

This plugin needs IBM-Cloud PyWren. It can be installed using `pip`:

`pip3 install pywren-ibm-cloud`

### Airflow Setup

At this point you can already copy the [example DAGs provided](https://github.com/aitorarjona/ibm-cf_airflow-plugin_exampleDAGs) to the `~/ariflow/dags` directory.

The basic setup is enough to execute the example DAGs:

```bash
# initialize the database
airflow initdb
# start the web server, default port is 8080
airflow webserver -p 8080
# start the scheduler
airflow scheduler
```

### IBM Cloud account credentials setup

Launch the Airflow web server and visit `localhost:8080` on your browser.
Navigate to the 'Connections' page under the 'Admin' menu located at the top bar.
Click on the 'Create' tab.
![enter image description here](https://i.ibb.co/rdWGC5Q/5.jpg)

Type **ibm_cf_config** inside the 'Conn Id' text box.
Then, paste the following configuration in the 'Extra' text box:
```python
{"pywren" : {"storage_bucket" : "BUCKET_NAME"},

"ibm_cf":  {"endpoint": "https://example.functions.cloud.ibm.com", 
            "namespace": "NAMESPACE", 
            "api_key" : "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"}, 

"ibm_cos": {"endpoint": "http://example.cloud-object-storage.appdomain.cloud", 
            "api_key": "API_KEY"}}
```
Please, fill in your credentials. Information of your Cloud Functions information can be found [here](https://cloud.ibm.com/openwhisk/namespace-settings), and for your Cloud Object Storage [here](https://cloud.ibm.com/objectstorage/crn%3Av1%3Abluemix%3Apublic%3Acloud-object-storage%3Aglobal%3Aa%2F827fd5191c5d42fd9a719542dffeb22e%3Aec0fe42c-4100-4d60-8c48-310c8624c311%3A%3A?paneId=credentials). The `storage_bucket` is the COS bucket where PyWren will save/load the data needed to run the functions. Make sure to have a bucket created and put its name in that field.

**Important: maintain the values inside double quotation marks.**

![enter image description here](https://i.ibb.co/4Z9KKg8/6.jpg)
Click 'Save' to exit.

## Running the example DAGs

Now you should be ready to run any example DAGs provided and start modifying them.
Remember to enable the DAG execution by toggling the 'On/Off' switch. Then, press 'Trigger DAG'.
![enter image description here](https://i.ibb.co/qND28gx/2.jpg)
After the execution, the results can be seen in the XCom page under the 'Admin' menu at the top bar. You can also have a look at the logs of each task to check further information on the task execution.
![enter image description here](https://i.ibb.co/xHH9wB6/4.jpg)

**Some example DAGs can be found [here](https://github.com/aitorarjona/ibm-cf_airflow-plugin_exampleDAGs).**

## Usage

### Operators

This plugin provides three new operators:
 - `IbmCloudFunctionsBasicOperator`
	Invokes a single function.
    
	| Parameter | Description |
	| ------------ | ------------- |
	| function     | Python callable |
	| op_args      | Function arguments, as a dictionary (key = parameter name, value = parameter value). |
	
	Example:
	```python
	def echo(x):
		return x
	```
	
	```python
	import echo from my_functions
	basic_task = IbmCloudFunctionsBasicOperator(
	    task_id='basic_task',
	    function=echo,
	    op_args={'x' : 'Hello'},
	    dag=dag,
	)
	```
	
	```python
	# Returns: 
	'Hello'
	```

 - `IbmCloudFunctionsMapOperator`
	Invokes multiple parallel tasks, as many as how much data is in parameter `iterdata`. It applies the function `map_function` to every element in `iterdata`:
    
	| Parameter | Description |
	| ------------ | ------------- |
	| map_function | Python callable. |
	| op_args | Function arguments as a dictionary. **Compulsory key: 'iterdata', where the value is the iterable parallelizable data (list, dictionary, bucket names, etc.).** |
	| chunk_size | Size (in Bytes) of the data chunks. Default: None (map per file). |
	| data_all_as_one | Upload the function's data as a single object. Default: True |
	| exclude_modules | Explicitly keep these modules from pickled dependencies. Default: None | 

	Example:
	```python
	def add(x, y):
		return x + y
	```
	
	```python
	from my_functions import add
	map_task = IbmCloudFunctionsMapOperator(
	    task_id='map_task',
	    map_function=add,
	    op_args={'iterdata' : [1, 2, 3], 'x' : 'iterdata', 'y' : 5},
	    dag=dag,
	)
	```
	```python
	# Returns: 
	[6, 7, 8]
	```
 - `IbmCloudFunctionsMapReduceOperator`
	Invokes multiple parallel tasks, as many as how much data is in parameter `iterdata`. It applies the function `map_function` to every element in `iterdata`. Finally, in invokes a `reduce_function` that gathers all the map results.
    
	| Parameter | Description |
	| ------------ | ------------- |
	| map_function | Python callable. |
	| reduce_function | Python callable. |
	| op_args | Function arguments as a dictionary. **Compulsory key: 'iterdata', where the value is the iterable parallelizable data (list, dictionary, bucket names, etc.).** |
	| chunk_size | Size (in Bytes) of the data chunks. Default: None (map per file). |
	| reducer_one_per_object | Invoke a reducer for every object after partitioning. Default: False |
	| reducer_wait_local | Wait for results locally. Default: False |
	| data_all_as_one | Upload the function's data as a single object. Default: True |
	| exclude_modules | Explicitly keep these modules from pickled dependencies. Default: None | 

	Example:
	```python
	def add(x, y):
		return x + y
		
	def mul(results):
		result = 1
		for n in results:
			result *= n
		return result			
	```
	
	```python
	from my_functions import add
	from my_functions import mult
	mapreduce_task = IbmCloudFunctionsMapReduceOperator(
	    task_id='mapreduce_task',
	    map_function=add,
	    reduce_funtion=mul,
	    op_args={'iterdata' : [1, 2, 3], 'x' : 'iterdata', 'y' : 5},
	    dag=dag,
	)
	```
	```python
	# Returns: 
	336
	```
### Using PyWren builtin features
All the features that provide PyWren IBM Cloud can also be used in this plugin.
To see some examples, please visit [PyWren's github repository](https://github.com/pywren/pywren-ibm-cloud/tree/master/examples).

#### Specifying a bucket name to gather iterable data for map_reduce tasks
A bucket name can be specified to obtain the iterbale data, as explained by [this](https://github.com/pywren/pywren-ibm-cloud/blob/master/examples/map_reduce_cos_bucket.py) pywren example. To specify a bucket name, include an entry to the operation arguments dictionary where the key is `'bucket'` and the value the name of the bucket.
```python
	map_reduce_with_bucket_objects = IbmCloudFunctionsMapOperator(
	    task_id='my_mapreduce_task,
	    map_function=add,
	    op_args={'bucket' : 'my_bucket'},
	    chunk_size=1024,
	    dag=dag
	)
```

### Redirect a task result value to a task input parameter
It is possible to redirect the output from a task to the input of another. To do so, the **value** of the **parameter key name** of the **function argument dictionary** must follow the syntax `'FROM_TASK:task_name'`, where `task_name` is the name of the task from where the output is retrieved.

Example:
```python
def generate_list():
    l = []
    for i in range(random.randint(1,100)):
        l.append(random.randint(1,100))
    return l
    
def add(x, y):
    return x + y
```

```python
from my_functions import generate_list
from my_functions import add
task1 = IbmCloudFunctionsBasicOperator(
    task_id='generate_list',
    function=generate_list,
    dag=dag,
)

task2 = IbmCloudFunctionsMapOperator(
    task_id='map_add',
    map_function=add,
    op_args={'iterdata' : 'FROM_TASK:generate_list', 'x' : 'iterdata', 'y' : 123},
    dag=dag,
)
```

_____________________
**Important note:** Functions must be declared outside the DAG, in a single module or within a directory. To access the functions inside the DAG, import them as regular modules.
_____________________

## License

[![Apache 2 license](https://img.shields.io/hexpm/l/plug.svg)](https://img.shields.io/hexpm/l/plug.svg)

