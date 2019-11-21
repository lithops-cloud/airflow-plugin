# Plotting meteorological data

The objective of this example is to demonstrate the CallAsync, Map and MapReduce operators, how IBM-PyWren features ease the use of managing data when using serverless functions, and how a bigdata analysis pipeline could be build using Airflow and IBM-PyWren.

## Workflow

The workflow consists in collecting different meteorological data (such as temperature or pressure) from a disorganized data source, then grouping the data by country and finally plotting the data to map figures using matplotlib.

#### Dataset
- [Source](https://openweathermap.org)
The dataset is a file containing 210000 JSON entries data with mateorological information of cities around the globe. The following sample is an example of the data for the city of Stockholm, Sweden, which is a single line of the dataset file:

```json
{
   "city":{
      "id":2673730,
      "name":"Stockholm",
      "findname":"STOCKHOLM",
      "country":"SE",
      "coord":{
         "lon":18.064899,
         "lat":59.332581
      },
      "zoom":5
   },
   "time":1554462368,
   "main":{
      "temp":281,
      "pressure":1019,
      "humidity":57,
      "temp_min":276.15,
      "temp_max":284.82
   },
   "wind":{
      "speed":4.1,
      "deg":20
   },
   "clouds":{
      "all":0
   },
   "weather":[
      {
         "id":800,
         "main":"Clear",
         "description":"clear sky",
         "icon":"01d"
      }
   ]
}
```

For this example, we will be plotting temperature, humidity and pressure data for Spain, Portugal, Italy, Germany and France.

```python
countries = ['ES', 'PT', 'IT', 'DE', 'FR']
plots = ['temp', 'humi', 'press']
```

**Step 1. Put dataset to COS - Call Async**

First, we need to put the dataset to COS in order to allow PyWren to treat it using its built-in data discovery and partitioner features. 
This step downloads the dataset, decompresses it and the uploads the raw content to COS. 
Since the dataset is rather small and can fit in the memory of a single function, we can make us of the CallAsync operator.

```python
def get_dataset(data_url, bucket, ibm_cos):
    # Download dataset
    res = requests.get(data_url)
    by = res.content

    # Decompress dataset
    with gzip.GzipFile(fileobj=io.BytesIO(by), mode='rb') as gzipf:
        x = gzipf.read()
    
    # Put file in COS
    ibm_cos.put_object(Bucket=bucket, Key='weather_data', Body=x)
```

```python
get_dataset = IbmPyWrenCallAsyncOperator(
    task_id='get_dataset',
    func=manage_data.get_dataset,
    data={'data_url' : 'http://bulk.openweathermap.org/sample/weather_16.json.gz', 'bucket' : bucket},
    dag=dag,
)
```

**2. Group data by country - Map**

Then, we map over the data obtained before, splitting the big object into small chunks.
PyWren splits our 210000 line file in chunks of 1 MB. In this case, PyWren splits the dataset in 71 chunks, so every function will process around 3000 lines in parallel.
The map function groups the data by country and saves the chunk to COS, it discards any data that don't correspond to the countries we want to treat.
The result will be 355 new files in COS (71 file per map function, per country grouped (5): ES/1, ..., ES/71, PT/1, ...) containing the data of the countries selected.

```python
def parse_data(obj, countries, bucket, id, ibm_cos):
    # Load chunk samples
    s = obj.data_stream.read().decode("utf-8")
    data = [json.loads(d) for d in s.splitlines()]

    samples = dict()
    
    # Group samples by countries, only the countries we want to treat
    for sample in data:
        if sample['city']['country'] in countries:
            if sample['city']['country'] not in samples:
                samples[sample['city']['country']] = list()
            samples[sample['city']['country']].append(json.dumps(sample))
    
    # Put grouped data to COS, every country will be in a separate object
    for k,v in samples.items():
        ibm_cos.put_object(Bucket=bucket, Key='{}/{}'.format(k, id), Body=json.dumps(v))
```

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
 
## DAG Definition
```python
get_dataset = IbmPyWrenCallAsyncOperator(
    task_id='get_dataset',
    func=manage_data.get_dataset,
    data={'data_url' : 'http://bulk.openweathermap.org/sample/weather_16.json.gz', 'bucket' : bucket},
    dag=dag,
)

parse_data = IbmPyWrenMapOperator(
        task_id='parse_data',
        map_function=manage_data.parse_data,
        map_iterdata='cos://{}/weather_data'.format(bucket),
        chunk_size=1024**2,
        extra_params=[countries, bucket],
        dag=dag
    )

get_dataset >> parse_data

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
            
        parse_data >> plot_task
```

## DAG Graph
![enter image description here](https://i.ibb.co/X2LhZkp/Screenshot-from-2019-11-19-21-01-59.png)


## Some results
 ![enter image description here](https://i.ibb.co/fCbf5Nz/image-ES-temp.png)
 ![enter image description here](https://i.ibb.co/NYrn7NM/image-DE-humi.png)
 ![enter image description here](https://i.ibb.co/thkRN0N/image-IT-press.png)
  
