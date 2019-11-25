# Plotting meteorological data

The objective of this example is to demonstrate the CallAsync, Map and MapReduce operators, how IBM-PyWren features ease the use of managing data when using serverless functions, and how a bigdata analysis pipeline could be build using Airflow and IBM-PyWren.

## Workflow

The workflow consists in collecting different meteorological data (such as temperature or pressure) from a disorganized data source, then grouping the data by country and finally plotting the data to map figures using matplotlib.

#### Dataset
- [Source](https://openweathermap.org)
The dataset is a file containing 210000 JSON entries data with meteorological information of cities around the globe. The following sample is an example of the data for the city of Stockholm, Sweden, which is a single line of the dataset file:

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

Finally, we need to gather all parsed data from the previous step, merge all chunks in the same list and then plot the data to the map. To do so, we will be using the MapReduce operator, which let us to first map a iterable data source and then reduce the results to one list. The map phase will consist on collecting the data from the previous map selecting only the attributes we want (for example, if we want to plot temperature, we only need temperature, latitude and longitude attributes). The reduce step consists on iterating over the mapped and merged results and scattering each coordinate onto the map, setting the color accordingly. We will be launching a MapReduce task for every country and for every attribute we want to plot. The final images are stored to COS.

Map function:
```python
def get_plot_data(obj, country, bucket, plot):
    samples = json.loads(obj.data_stream.read())

    res_l = list()
    for sample in samples:
        sample_json = json.loads(sample)
        res = dict()
        # Get latitude and longitude attributes
        res['lon'] = sample_json['city']['coord']['lon']
        res['lat'] = sample_json['city']['coord']['lat']
        # Get only the attribute we want to plot
        if plot == 'temp':
            res['temp'] = sample_json['main']['temp'] - 273
        elif plot == 'humi':
            res['humi'] = sample_json['main']['humidity']
        elif plot == 'press':
            res['press'] = sample_json['main']['pressure']
        else:
            raise Exception
        res_l.append(res)
    return res_l
```
Reduce function:
```python
def plot_temp(results, ibm_cos):
    country = os.environ.get('country')
    bucket = os.environ.get('bucket')
    plot = os.environ.get('plot')

    all_results = [item for sublist in results for item in sublist]
    
    llcrnrlon=min(all_results, key=lambda sample : sample['lon'])['lon']
    llcrnrlat=min(all_results, key=lambda sample : sample['lat'])['lat']
    urcrnrlon=max(all_results, key=lambda sample : sample['lon'])['lon']
    urcrnrlat=max(all_results, key=lambda sample : sample['lat'])['lat']

    lons = [sample['lon'] for sample in all_results]
    lats = [sample['lat'] for sample in all_results]

    fig = plt.gcf()
    fig.set_size_inches(8, 6.5)

    m = Basemap(
        llcrnrlon=llcrnrlon,
        llcrnrlat=llcrnrlat,
        urcrnrlon=urcrnrlon,
        urcrnrlat=urcrnrlat,
        projection='merc',
        resolution='h')
    
    x, y = m(lons, lats)

    if plot == 'temp':
        cmap = plt.get_cmap('coolwarm')
        data = [sample['temp'] for sample in all_results]
        plt.scatter(x, y, 2.5, alpha=0.5, c=data, zorder=2, cmap=cmap)
        cbar = plt.colorbar()
        cbar.ax.set_ylabel('Temperature (ºC)')
        plt.title('{} temperature'.format(country))
    elif plot == 'humi':
        cmap = plt.get_cmap('Blues')
        data = [sample['humi'] for sample in all_results]
        plt.scatter(x, y, 2.5, alpha=0.5, c=data, zorder=2, cmap=cmap)
        cbar = plt.colorbar()
        cbar.ax.set_ylabel('Humidity (%)')
        plt.title('{} humidity (%)'.format(country))
    elif plot == 'press':
        cmap = plt.get_cmap('seismic')
        data = [sample['press'] for sample in all_results]
        plt.scatter(x, y, 2.5, alpha=0.5, c=data, zorder=2, cmap=cmap)
        cbar = plt.colorbar()
        cbar.ax.set_ylabel('Pressure (hPa)')
        plt.title('{} pressure (hPa)'.format(country))
    else:
        raise Exception()

    m.drawcountries(color="black", zorder=3)
    m.drawmapboundary(fill_color='cornflowerblue')
    m.fillcontinents(color='moccasin', lake_color='cornflowerblue', zorder=1)
    buff = BytesIO()
    plt.savefig(buff, dpi=100)
    buff.seek(0)

    key = 'image_{}_{}.png'.format(country, plot)
    ibm_cos.put_object(Bucket=bucket, Key=key, Body=buff)
    return key
```

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
  
