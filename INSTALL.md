
## Installation
**Note:** These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.


TODO: Wait for cloudbutton toolkit to be available in PyPi

### Initial requirements
In order to execute functions on IBM Cloud using PyWren, the following requirements are needed:

- An IBM Cloud Functions [account](https://cloud.ibm.com/openwhisk/). 
- An IBM Cloud Object Storage [account](https://www.ibm.com/cloud/object-storage).
- Python 3.5 or newer.

### Installing Apache Airflow

Use `pip` to install the last stable version of Apache Airflow.

```
$ pip install apache-airflow
```

### Installing Cloudbutton Plugin

You can simply install the pugin using the setup.py script:

`$ python3 setup.py install --user`

Alternatively, you can move the module into airflow's plugin folder:

`$ cp -r cloudbutton_airflow_plugin ~/airflow/plugins`

Note: Only one of the two options above descripted has to be done.

_Optional_: Move the example DAGs provided to the `dags` folder:

`$ cp -r example_dags ~/airflow/dags/`

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

### Create PyWren config in Airflow's connections

By default, Cloudbutton plugin will use the configuraton file provided in the home directory.

However, using Airflow connections it is possible define another configuration specificaly for the Airflow plugin:

Navigate to `localhost:8080` on your browser.

![enter image description here](https://i.ibb.co/rdWGC5Q/5.jpg)

Type **cloudbutton_engine_config** inside the 'Conn Id' text box.
Then, paste a custom Cloudbutton configuration in JSON format into the 'Extra' text box, for example:

```python
{"cloudbutton" : {"storage_bucket" : "BUCKET_NAME"},

"ibm_cf":  {"endpoint": "https://example.functions.cloud.ibm.com", 
            "namespace": "NAMESPACE", 
            "api_key" : "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"}, 

"ibm_cos": {"endpoint": "http://example.cloud-object-storage.appdomain.cloud", 
            "api_key": "API_KEY"}}
```

![enter image description here](https://i.ibb.co/4Z9KKg8/6.jpg)

Click 'Save' to exit.
