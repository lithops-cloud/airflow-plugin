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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.lithops_airflow_plugin import (
    LithopsCallAsyncOperator,
    LithopsMapOperator,
    LithopsMapReduceOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

import random
import lithops_example_functions as example_functions

args = {
    'owner': 'lithops',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='LithopsTest',
    default_args=args,
    schedule_interval=None,
)

gen_number = PythonOperator(
    task_id='gen_number',
    python_callable=lambda: random.randint(1, 100),
    dag=dag
)

add_num = LithopsCallAsyncOperator(
    task_id='add_num',
    func=example_functions.add_num,
    data={'b': 7},
    data_from_task={'a': 'gen_number'},
    dag=dag
)

gen_list = PythonOperator(
    task_id='gen_list',
    python_callable=lambda: [random.randint(1, 100) for _ in range(10)],
    dag=dag
)

mult_num_map = LithopsMapOperator(
    task_id='mult_num_map',
    map_function=example_functions.add_num,
    iterdata_from_task={'a': 'gen_list'},
    extra_args={'b': 10},
    dag=dag
)

map_reduce = LithopsMapReduceOperator(
    task_id='map_reduce',
    map_function=example_functions.mul_num,
    reduce_function=example_functions.add_num,
    map_iterdata=[1, 2, 3, 4, 5],
    extra_args={'b': 10},
    dag=dag
)

gen_number >> add_num
gen_list >> mult_num_map
