# NDVI Calculation using Cloudbutton and Airflow

TODO

![image](images/ndvi.png?raw=true "NDVI")

Thanks to Airflow extensibility using the operator abstraction and centralized storage such as S3, we can now seamlessly mix serverful and serverless tasks in the same workflow. In this case, we are using AWS Batch operator to run a task that requires more memory and time that a serverless function can provide, so they are executed transparently in Docker containers that run on EC2 instances without memory or time constraints. The rest of the tasks are short and light memory-wise, so they can be executed as serverless functions using Cloudbutton toolkit.

In addition, Airflow allows as to benefit from great workflow fault tolerance, logging, and observability features. One key concept of Airflow is its "configuration as code" paradigm used to compose DAGs as a Python script, which is very convenient to design and maintain a complex scientific workflow. Finally, Airflow GUI makes it comfortable to schedule and monitor DAG runs.

## Setup

Setup a Python virtual environment and install the required modules:
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Open the `data_preparation.ipynb` notebook:
```
jupyter notebook
```

Follow the instructions detailed in the notebook to select the data to be processed.

Copy the DAG file and the processing functions to Airflow's dag folder:
```
cp airflow/NDVIcalculation.py airflow/ndvi_calc.py ~/airflow/dags
```
The DAG should now appear in the Airflow GUI's DAG list.

## DAG overview
![image](images/airflow_dag.png?raw=true "DAG")

