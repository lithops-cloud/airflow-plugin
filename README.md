# Cloudbutton Apache Airflow Plugin

This repository contains an Apache Airflow Plugin that implements new operators to easily deploy serverless functions tasks on IBM Cloud Functions.
Cloudbutton toolkit is a Python multicloud library for running serverless jobs. Cloudbutton toolkit interfaces Python's multiprocessing module to transparently run processes over serverless functions as if they were executed localy. This plugin benefits from this library to run highly parallelizable Airflow tasks as serverless functions, achieving higher performance for big data analysis workflows whithout consuming all the resources of the cluser where Airflow is running on.

- Apache Airflow: https://github.com/apache/airflow
- Cloudbutton toolkit: https://github.com/cloudbutton/cloudbutton
- CloudButton Project: http://cloudbutton.eu/

## Contents

1. [Installation](https://github.com/cloudbutton/airflow-plugin/blob/master/INSTALL.md)
2. [Usage](#usage)
3. [Examples](https://github.com/cloudbutton/airflow-plugin/tree/master/example_dags)

## Usage

### Operators

This plugin provides three new operators.

_____________________
**Important note:** Due to the way Airflow manages DAGs, the callables passed to the Cloudbutton operators can not be declared in the DAG definition script. Instead, they must be declared inside a separate file or module. To access the functions from the DAG file, import them as regular modules.
_____________________

 - **CloudbuttonCallAsyncOperator**
	
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
	from my_functions import add
	my_task = CloudbuttonCallAsyncOperator(
	    task_id='add_task',
	    func=add,
	    data={'x' : 1, 'y' : 3},
	    dag=dag,
	)
	```
	
	```python
	# Returns: 
	4
	```
	
	```python
	from my_functions import add
	basic_task = CloudbuttonAsyncOperator(
	    task_id='add_task_2',
	    func=add,
	    data={'x' : 4},
	    data_from_task={'y' : 'add_task_1'},
	    dag=dag,
	)
	```
	
	```python
	# Returns: 
	8
	```

 - **CloudbuttonMapOperator**
	
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
	map_task = CloudbuttonMapOperator(
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
	
 - **CloudbuttonMapReduceOperator**
	
	It invokes multiple parallel tasks, as many as how much data is in parameter `map_iterdata`. It applies the function `map_function` to every element in `iterdata`. Finally, a single `reduce_function` is invoked that gathers all the map results.
    
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
	from my_functions import add, mult
	mapreduce_task = CloudbuttonMapReduceOperator(
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
  | cloudbutton_engine_config | Cloudbutton engine config, as a dictionary | `{}` | `dict` |
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
