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

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.cloudbutton_airflow_plugin import (
    CloudbuttonMapOperator,
    CloudbuttonCallAsyncOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

import ndvi_calc

args = {
    'owner': 'cloudbutton',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='NDVI_Calculation',
    default_args=args,
    schedule_interval=None,
)

# PARAMETERS
AWS_BATCH_JOB_DEFINITION = 'arn:aws:batch:us-east-1:1234567890:job-definition/sen2cor:1'
AWS_BATCH_JOB_QUEUE = 'my-batch-queue'
BUCKET = 'my-bucket'
SPLITS = 2

start = DummyOperator(task_id='start',
                      dag=dag)

end = DummyOperator(task_id='end',
                    dag=dag)

tiles_meta = Variable.get('SENTINEL_TILES', deserialize_json=True)

if tiles_meta:

    #######################################################
    # Preare data and calculate NDVI index for every tile #
    #######################################################

    get_tile_id = CloudbuttonMapOperator(
        task_id='get_tile_id',
        cloudbutton_engine_config={
            'runtime': 'aitorarjona/geospatial-runtime:3.8-v2'
        },
        map_function=ndvi_calc.get_tile_id,
        map_iterdata=tiles_meta,
        extra_args=(BUCKET,),
        runtime_memory=2048,
        dag=dag
    )

    start >> get_tile_id

    sen2cor_tasks = []
    for tile in tiles_meta:
        task_name = 'SEN2COR_{}'.format(tile.split('.')[0])
        sen2cor_task = AWSBatchOperator(task_id=task_name,
                                        job_name=task_name,
                                        job_definition=AWS_BATCH_JOB_DEFINITION,
                                        job_queue=AWS_BATCH_JOB_QUEUE,
                                        overrides={'command': [tile]},
                                        parameters={},
                                        dag=dag)
        # sen2cor_task = DummyOperator(task_id=task_name, dag=dag)

        sen2cor_tasks.append(sen2cor_task)
    
    get_tile_id >> sen2cor_tasks

    ndvi_index = CloudbuttonMapOperator(
        task_id='ndvi_index',
        cloudbutton_engine_config={
            'runtime': 'aitorarjona/geospatial-runtime:3.8-v2'
        },
        map_function=ndvi_calc.map_ndvi,
        map_iterdata=tiles_meta,
        extra_args=(BUCKET, ),
        runtime_memory=2048,
        dag=dag
    )

    sen2cor_tasks >> ndvi_index

    ###########################################
    # Compute average NDVI per tile per month #
    ###########################################

    group_tiles = PythonOperator(
        task_id='group_tiles',
        python_callable=ndvi_calc.group_tiles,
        op_kwargs={'items': "{{ ti.xcom_pull(task_ids='get_tile_id') }}",
                   'geotiff_items': "{{ ti.xcom_pull(task_ids='ndvi_index') }}"},
        dag=dag
    )

    average_ndvi_month = CloudbuttonMapOperator(
        task_id='average_ndvi_month',
        cloudbutton_engine_config={
            'runtime': 'aitorarjona/geospatial-runtime:3.8-v2'
        },
        map_function=ndvi_calc.avg_map_ndvi,
        map_iterdata=[],
        iterdata_from_task='group_tiles',
        extra_args={'bucket': BUCKET},
        runtime_memory=2048,
        dag=dag
    )

    ndvi_index >> group_tiles >> average_ndvi_month

    ################################
    # Average NDVI using shapefile #
    ################################

    split_tiles_shape = PythonOperator(
        task_id='split_tiles_shape',
        python_callable=ndvi_calc.split_tiles,
        op_kwargs={
            'items': "{{ ti.xcom_pull(task_ids='get_tile_id') }}",
            'geotiff_items': "{{ ti.xcom_pull(task_ids='ndvi_index') }}",
            'splits': SPLITS},
        dag=dag
    )

    average_shape_ndvi = CloudbuttonMapOperator(
        task_id='average_shape_ndvi',
        cloudbutton_engine_config={
            'runtime': 'aitorarjona/geospatial-runtime:3.8-v2'
        },
        map_function=ndvi_calc.avg_shape_ndvi,
        iterdata_from_task='split_tiles_shape',
        extra_args={'bucket': BUCKET},
        runtime_memory=2048,
        dag=dag
    )

    gather_blocks_shape_ndvi = CloudbuttonMapOperator(
        task_id='gather_blocks_shape_ndvi',
        cloudbutton_engine_config={
            'runtime': 'aitorarjona/geospatial-runtime:3.8-v2'
        },
        map_function=ndvi_calc.gather_blocks,
        iterdata_from_task={'item': 'ndvi_index'},
        extra_args={'splits': SPLITS, 'bucket': BUCKET},
        runtime_memory=2048,
        dag=dag
    )

    ndvi_index >> split_tiles_shape >> average_shape_ndvi >> gather_blocks_shape_ndvi

    #############################################################################
    # Compute average NDVI by shape for previously NDVI tiles averaged by MONTH #
    #############################################################################
    
    split_tiles_average_month = PythonOperator(
        task_id='split_tiles_average_month',
        python_callable=ndvi_calc.split_tiles,
        op_kwargs={
            'items': "{{ ti.xcom_pull(task_ids='get_tile_id') }}",
            'geotiff_items': "{{ ti.xcom_pull(task_ids='average_ndvi_month') }}",
            'splits': SPLITS},
        dag=dag
    )

    average_shape_ndvi_month = CloudbuttonMapOperator(
        task_id='average_shape_ndvi_month',
        cloudbutton_engine_config={
            'runtime': 'aitorarjona/geospatial-runtime:3.8-v2'
        },
        map_function=ndvi_calc.avg_shape_ndvi,
        iterdata_from_task='split_tiles_average_month',
        extra_args={'bucket': BUCKET},
        runtime_memory=2048,
        dag=dag
    )

    gather_blocks_shape_ndvi_month = CloudbuttonMapOperator(
        task_id='gather_blocks_shape_ndvi_month',
        cloudbutton_engine_config={
            'runtime': 'aitorarjona/geospatial-runtime:3.8-v2'
        },
        map_function=ndvi_calc.gather_blocks,
        iterdata_from_task={'item': 'ndvi_index'},
        extra_args={'splits': SPLITS, 'bucket': BUCKET},
        runtime_memory=2048,
        dag=dag
    )

    average_ndvi_month >> split_tiles_average_month >> average_shape_ndvi_month >> gather_blocks_shape_ndvi_month
    
    ##############################################
    # Delete temporary files from object storage #
    ##############################################
    
    clean_tmp = CloudbuttonCallAsyncOperator(
    	task_id='clean_tmp',
    	func=ndvi_calc.clean_tmp,
    	data={'bucket': BUCKET},
    	dag=dag
    )
    
    [gather_blocks_shape_ndvi_month, gather_blocks_shape_ndvi] >> clean_tmp >> end
    
else:
    start >> end
