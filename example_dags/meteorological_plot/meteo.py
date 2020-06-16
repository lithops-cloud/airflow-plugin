# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import airflow

from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.cloudbutton_airflow_plugin import (
    CloudbuttonCallAsyncOperator,
    CloudbuttonMapOperator,
    CloudbuttonMapReduceOperator
)


from functions import manage_data, plot_map

args = {'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2),
        'provide_context': True}

countries = ['ES', 'PT', 'IT', 'DE', 'FR']
plots = ['temp', 'humi', 'press']
bucket = 'aitor-data'

dag = airflow.models.DAG(dag_id='pywren_weather_example',
                         default_args=args,
                         schedule_interval=None)

get_dataset = CloudbuttonCallAsyncOperator(
    task_id='get_dataset',
    func=manage_data.get_dataset,
    data={'data_url': 'http://bulk.openweathermap.org/sample/weather_16.json.gz',
          'bucket': bucket},
    dag=dag,
)

parse_data = CloudbuttonMapOperator(
    task_id='parse_data',
    map_function=manage_data.parse_data,
    map_iterdata='cos://{}/weather_data'.format(bucket),
    chunk_size=1024**2,
    extra_args=[countries, bucket],
    dag=dag
)

get_dataset >> parse_data

for plot in plots:
    for country in countries:
        plot_task = CloudbuttonMapReduceOperator(
            task_id='{}_plot_{}'.format(country, plot),
            pywren_executor_config={
                'runtime_memory': 2048,
                'runtime': 'aitorarjona/python3.6_pywren_matplotlib-basemap:1.0'},
            map_function=manage_data.get_plot_data,
            map_iterdata='cos://{}/{}/'.format(bucket, country),
            reduce_function=plot_map.plot_temp,
            extra_args=[country, bucket, plot],
            extra_env={'country': country, 'bucket': bucket, 'plot': plot},
            dag=dag)

        parse_data >> plot_task
