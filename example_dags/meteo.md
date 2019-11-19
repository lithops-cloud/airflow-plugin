# Plotting meteorological data

## Workflow

The workflow consists of collecting different meteorological data (such as temperature or pressure) from an unstructured and disorganized data source, then grouping the data by country and finally plot the data to map figures using matplotlib.

```python
countries = ['ES', 'PT', 'IT', 'DE', 'FR']
plots = ['temp', 'humi', 'press']
```

It has three well differentiated phases:

**1. Put dataset to COS - Call Async**

Downloads the dataset, decompresses it and the uploads the raw content to COS.

```python
get_dataset = IbmPyWrenCallAsyncOperator(
    task_id='get_dataset',
    func=manage_data.get_dataset,
    data={'data_url' : 'http://bulk.openweathermap.org/sample/weather_16.json.gz', 'bucket' : bucket},
    dag=dag,
)
```

**2. Group data by country - Map**

Maps over the raw data obtained before, splitting the big object into small chunks.
It groups the data by country and saves the chunk to COS, it discards any data that don't correspond to the countries we want to treat.

```python
parse_data = IbmPyWrenMapOperator(
    task_id='parse_data',
    map_function=manage_data.parse_data,
    map_iterdata='cos://{}/weather_data'.format(bucket),
    chunk_size=1024**2,
    extra_params=[countries, bucket],
    dag=dag
)
```

**3. Gather the parameter to plot and plot the data - MapReduce**

First, it maps over the data grouped by country on the step before and selects only the attributes we want (for example, if we want to plot temperature, we only need temperature, latitude and longitude attributes).
Then, it reduces the results by iterating over them and plotting each coordinate onto the map, seting the color accordingly.
The final images are stored in COS.

```python
for plot in plots:
    for country in countries:
        plot_task = IbmPyWrenMapReduceOperator(
            task_id='{}_plot_{}'.format(country, plot),
            pywren_executor_config={
                'runtime_memory' : 2048,
                'runtime' : 'aitorarjona/python3.6_pywren_matplotlib-basemap:1.0'},
            map_function=manage_data.get_plot_data,
            map_iterdata='cos://{}/{}/'.format(bucket, country),
            reduce_function=plot_map.plot_temp,
            extra_params=[country, bucket, plot],
            extra_env={'country' : country, 'bucket' : bucket, 'plot' : plot},
            dag=dag)
 ```
 
 ## DAG graph
 ![enter image description here](https://i.ibb.co/X2LhZkp/Screenshot-from-2019-11-19-21-01-59.png)


## Some results
 ![enter image description here](https://i.ibb.co/fCbf5Nz/image-ES-temp.png)
 ![enter image description here](https://i.ibb.co/NYrn7NM/image-DE-humi.png)
 ![enter image description here](https://i.ibb.co/thkRN0N/image-IT-press.png)
  
