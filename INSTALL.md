
## Installation
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Initial requirements
The Lithops package must be installed in the same Python environment used to run Airflow:

- [Lithops](https://github.com/lithops-cloud/lithops)

### Installing Apache Airflow

Use `pip` to install the last stable version of Apache Airflow.

```
$ pip install apache-airflow
```

### Installing Lithops Plugin

There are two alternatives to install the Lithops plugin. Choose one:

- Install the pugin using the setup.py script:
```
$ python3 setup.py install --user
```

- Move the module into Airflow's plugin folder:
```
$ cp -r lithops_airflow_plugin ~/airflow/plugins
```

Note: Only one of the two options above descripted has to be done.

_Optional_: Move the example DAGs provided to the `dags` folder:

`$ cp -r example_dags/meteorological_plot ~/airflow/dags/`

### Airflow Setup

The basic setup is enough to execute the example DAGs:

```bash
# initialize the database
airflow initdb
# start the web server, default port is 8080
airflow webserver -p 8080
# start the scheduler
airflow scheduler
```

### Configure Lithops as a Airflow connection (optional)

By default, Lithops plugin will use the configuraton file provided in the home directory.

However, using Airflow connections it is possible define another configuration specificaly for the Airflow plugin:

Navigate to `localhost:8080` on your browser.

![enter image description here](https://i.ibb.co/rdWGC5Q/5.jpg)

Type **lithops_engine_config** inside the 'Conn Id' text box.
Then, paste a custom Lithops configuration in JSON format into the 'Extra' text box, for example:

```python
{"lithops" : {"storage_bucket" : "BUCKET_NAME"},

"ibm_cf":  {"endpoint": "https://example.functions.cloud.ibm.com", 
            "namespace": "NAMESPACE", 
            "api_key" : "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"}, 

"ibm_cos": {"endpoint": "http://example.cloud-object-storage.appdomain.cloud", 
            "api_key": "API_KEY"}}
```

![enter image description here](https://i.ibb.co/4Z9KKg8/6.jpg)

Click 'Save' to exit.
