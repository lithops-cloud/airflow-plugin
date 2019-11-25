
## Installation
**Note:** These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

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

### Installing IBM Cloud Functions Plugin

Move to Airflow home directory. The default location is `~/airflow`:

`$ cd ~/airflow`

Create the `plugins` directory, and cd into it:

`$ mkdir plugins`

`$ cd plugins`

Clone the plugin repository into it:

`$ git clone https://github.com/aitorarjona/ibm-pywren_airflow-plugin`

This plugin needs IBM-Cloud PyWren. It can be installed using `pip`:

`$ pip install pywren-ibm-cloud`

_Optional_: Move the example DAGs provided to the `dags` folder:

`$ mv example_dags ~/airflow/dags/`

Don't forget to remove the `example-dags` folder or move it to the Airflow's dag folder (`~/airflow/dags/`):

`$ rm -r example_dags` 

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

Navigate to `localhost:8080` on your browser.

![enter image description here](https://i.ibb.co/rdWGC5Q/5.jpg)



Type **ibm_pywren_config** inside the 'Conn Id' text box.
Then, paste the following configuration in the 'Extra' text box:

```python
{"pywren" : {"storage_bucket" : "BUCKET_NAME"},

"ibm_cf":  {"endpoint": "https://example.functions.cloud.ibm.com", 
            "namespace": "NAMESPACE", 
            "api_key" : "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"}, 

"ibm_cos": {"endpoint": "http://example.cloud-object-storage.appdomain.cloud", 
            "api_key": "API_KEY"}}
```

Please, fill in your credentials. Information of your Cloud Functions information can be found [here](https://cloud.ibm.com/openwhisk/namespace-settings), and for your Cloud Object Storage [here](https://cloud.ibm.com/objectstorage/crn%3Av1%3Abluemix%3Apublic%3Acloud-object-storage%3Aglobal%3Aa%2F827fd5191c5d42fd9a719542dffeb22e%3Aec0fe42c-4100-4d60-8c48-310c8624c311%3A%3A?paneId=credentials). The `storage_bucket` is the COS bucket where PyWren will save/load the data needed to run the functions. Make sure to have a bucket created and put its name in that field. This bucket needs to be in the same region as the funcions namespace.

**Important: maintain the values inside double quotation marks.**

![enter image description here](https://i.ibb.co/4Z9KKg8/6.jpg)

Click 'Save' to exit.
