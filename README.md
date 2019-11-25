# IBM PyWren: Apache Airflow Plugin

This repository contains an Apache Airflow Plugin that provides new operators to easily deploy serverless functions tasks on IBM Cloud Functions. 
IBM-Cloud PyWren offers the possibility to easily deploy map and map reduce jobs that can compute large amount of data from IBM Cloud Object Storage using thousands of parallel serverless functions. 
This plugin includes three new operators to easily unload the heavy work in your DAGs related to data computation, that otherwise would be executed in the Airflow cluster, by moving them to serverless functions.

- Apache Airflow: https://github.com/apache/airflow
- PyWren IBM Cloud: https://github.com/pywren/pywren-ibm-cloud
- CloudButton Project: http://cloudbutton.eu/


✓ Tested on *Airflow v1.10.3* and *IBM-PyWren 1.1.2*


## Contents

1. [Installation](https://github.com/aitorarjona/ibm-pywren_airflow-plugin/blob/dev/INSTALL.md)
2. [Usage](#usage)
3. [Examples](https://github.com/aitorarjona/ibm-pywren_airflow-plugin/tree/dev/example_dags)

## Usage

### Operators

This plugin provides three new operators.
_____________________
**All operators support IBM-PyWren functionalities, such as mapping over a bucket, or splitting a COS object in several chunks. For more details please refer to the [IBM-PyWren documentation](https://github.com/pywren/pywren-ibm-cloud).**
_____________________
**Important note:** Functions must be declared outside the DAG, in a single module or within a directory. To access the functions inside the DAG, import them as regular modules.
_____________________

 - **IbmPyWrenCallAsyncOperator**
	
	It invokes a single function.
 
	| Parameter | Description | Default |
	| --- | --- | --- |
	| func | Python callable | _mandatory_ |
	| data | Key word arguments | `{}` |
	| data_from_task | Get the output from another task as an input parameter for this function | `None` |

	Example:
	```python
	def add(x, y):
		return x + y
	```
	
	```python
	import echo from my_functions
	basic_task = IbmPyWrenCallAsyncOperator(
	    task_id='add_task_1',
	    func=echo,
	    data={'x' : 1, 'y' : 3},
	    dag=dag,
	)
	```
	
	```python
	# Returns: 
	4
	```
	
	```python
	import echo from my_functions
	basic_task = IbmPyWrenCallAsyncOperator(
	    task_id='add_task_2',
	    func=echo,
	    data={'x' : 4},
	    data_from_task={'y' : 'add_task_1'},
	    dag=dag,
	)
	```
	
	```python
	# Returns: 
	8
	```

 - **IbmPyWrenMapOperator**
	
	It invokes multiple parallel tasks, as many as how much data is in parameter `map_iterdata`. It applies the function `map_function` to every element in `map_iterdata`:
    
	| Parameter | Description | Default | Type |
	| ------------ | ------------- | ------ | ---- |
	| map_function | Python callable. | _mandatory_ | `callable` |
	| map_iterdata | Iterable. Invokes a function for every element in `iterdata` | _mandatory_ | _Has to be iterable_ |
	| iterdata_form_task | Gets the input iterdata from another function's output | `None` | _Has to be iterable_ |
	| extra_params | Adds extra key word arguments to map function's signature | `None` | `dict` |
	| chunk_size | Splits the object in chunks, and every chunk gets this many bytes as input data (on invocation per chunk) | `None` | `int` |
	| chunk_n | Splits the object in N chunks (on invocation per chunk) | `None` | `int` |
	| remote_invocation | Activates pywren's remote invocation functionality | False | `bool` |
	| invoke_pool_threads | Number of threads to use to invoke | `500` | `int` |

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
	    map_iterdata=[1, 2, 3],
	    extra_params={'y' : 1},
	    dag=dag,
	)
	```
	```python
	# Returns: 
	[2, 3, 4]
	```
	
 - **IbmPyWrenMapReduceOperator**
	
	It invokes multiple parallel tasks, as many as how much data is in parameter `map_iterdata`. It applies the function `map_function` to every element in `iterdata`. Finally, in invokes a `reduce_function` that gathers all the map results.
    
	| Parameter | Description | Default | Type |
	| ------------ | ------------- | ------ | ---- |
	| map_function | Python callable. | _mandatory_ | `callable` |
	| map_iterdata | Iterable. Invokes a function for every element in `iterdata` | _mandatory_ | _Has to be iterable_ |
	| reduce_function | Python callable. | _mandatory_ | `callable` |
	| iterdata_form_task | Gets the input iterdata from another function's output | `None` | _Has to be iterable_ |
	| extra_params | Adds extra key word arguments to map function's signature | `None` | `dict` |
	| map_runtime_memory | Memory to use to run the map functions | Loaded from config | `int` |
	| reduce_runtime_memory | Memory to use to run the reduce function | Loaded from config | `int` |
	| chunk_size | Splits the object in chunks, and every chunk gets this many bytes as input data (on invocation per chunk). 'None' for processing the whole file in one function activation | `None` | `int` |
	| chunk_n | Splits the object in N chunks (on invocation per chunk). 'None' for processing the whole file in one function activation | `None` | `int` |
	| remote_invocation | Activates pywren's remote invocation functionality | False | `bool` |
	| invoke_pool_threads | Number of threads to use to invoke | `500` | `int` |
	| reducer_one_per_object | Set one reducer per object after running the partitioner | `False` | `bool` |
	| reducer_wait_local | Wait for results locally | `False` | `bool` |

	Example:
	```python
	def add(x, y):
		return x + y
		
	def mult_array(results):
		result = 1
		for n in results:
			result *= 2
		return result			
	```
	
	```python
	from my_functions import add
	from my_functions import mult
	mapreduce_task = IbmCloudFunctionsMapReduceOperator(
	    task_id='mapreduce_task',
	    map_function=add,
	    reduce_funtion=mul,
	    map_iterdata=[1, 2, 3],
	    extra_params={'y' : 1},
	    dag=dag,
	)
	```
	```python
	# Returns: 
	18
	```

  ### Inherited parameters
  All operators inherit a common PyWren operator that has the following parameters:
  
  | Parameter | Description | Default | Type |
  | --- | --- | --- | --- |
  | pywren_executor_config | Pywren executor config, as a dictionary | `{}` | `dict` |
  | wait_for_result | Waits for function/functions completion | `True` | `bool` |
  | fetch_result | Downloads function/functions results upon completion | `True` | `bool` |
  | clean_data | Deletes PyWren metadata from COS | `False` | `bool` |
  | extra_env | Adds environ variables to function's runtime | `None` | `dict` |
  | runtime_memory | Runtime memory, in MB | `256` | `int` |
  | timeout | Time that the functions have to complete their execution before raising a timeout | Default from config | `int` |
  | include_modules | Explicitly pickle these dependencies | `[]` | `list` |
  | exclude_modules | Explicitly keep these modules from pickled dependencies | `[]` | `list` |

## License

[![Apache 2 license](https://img.shields.io/hexpm/l/plug.svg)](https://img.shields.io/hexpm/l/plug.svg)
