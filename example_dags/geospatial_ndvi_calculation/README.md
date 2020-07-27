# NDVI Calculation using Cloudbutton and Airflow

![image](images/ndvi.png?raw=true "NDVI")

As a demonstration of a real-world scenario where mixing serverful and serverless computation is required, we have implemented a scientific geospatial data analysis workflow. The workflow is about the computation of the Normalized Difference Vegetation Index (NDVI) of a set of terrestrial plots using geospatial data from the ESA produced by the SENTINEL$2$ satellite.

The first phase of the process that requires the download and filtering of the base tile data. This process is slow (around $30$ minutes), needs more memory than current serverless function services can provide and it uses ``blackbox'' legacy software from the ESA and parallelization is not possible. However, atmospheric and geometric correction is necessary and recommended to minimize distortions in the image that may be caused by clouds or alterations in satellite movement, sensor failure, etc. After the pre-processing and optimization of the data, it can be uploaded to a disaggregated object storage such as IBM Cloud Object Storage.

The NDVI calculation has to be calculated every month of the year, in order to have a reference of the evolution of the crops in the study area.

We also use the SIGPAC (Geographic Information System Common Agriculture Policy) plot, where the different crops that occur in the territory are officially reflected. This vector information also provides the land plots reference limits, so it constitutes very useful information to compare with the NDVI. This allows discrimination between arable land and tree crops, which means that, knowing the type of crop, the $K_c$ value necessary to calculate their evapotranspiration can be established.

Finally, joining the NDVI (raster) and the SIGPAC (vector) plot, it is possible to calculate the mean of NDVI values for each of the plots.

To orchestrate this workflow we used Apache Airflow airflow. Airflow is a DAG orchestration platform that has a wide service integration, including multiple cloud provider services. A strong point of Airflow is its extensibility using the operator abstraction. The operator takes care of where or how a task is executed. We can develop new operators using plugins so that we can use the Cloudbutton Toolkit to incorporate serverless tasks in a DAG. Using different task operators in Airflow, we can now seamlessly mix serverful and serverless tasks in the same workflow. In this case, we are using the AWS Batch operator to run the task that requires running in a server instance instead of on a serverless function. In this sense, these tasks are transparently executed in Docker containers that run on EC2 instances without memory or time constraints, or in serverless functions, where they take advantage of low cold start and high paralelism.

In addition, Airflow allows us to benefit from great workflow fault tolerance, logging, and observability features. One key concept of Airflow is its ``configuration as code'' paradigm used to compose DAGs as a Python script, which is very convenient to design, maintain and reuse a complex scientific workflow. Also, as this workflow has to be executed every month, it can be set up using cron triggers so it can be executed automatically. Finally, Airflow GUI makes it comfortable to schedule and monitor DAG runs.

#### Authors
[Answare](http://www.answare-tech.com/en/home/):
- Tonny Velin
- Pablo David Muñoz
- Cristina Castillo García
- Carlos A. Pineda Martínez

CLOUDLAB Universitat Rovira i Virgili:
- Aitor Arjona

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

