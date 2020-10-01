#
# Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lithops_airflow_plugin",
    version="1.0.0",
    author="Cloudlab Team",
    author_email="cloudlab@urv.cat",
    description="Lithops plugin for Apache Airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lithops/airflow-plugin",
    install_requires=[
        "lithops>=2.0.0 "
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    entry_points = {
        'airflow.plugins': [
            'plugin = lithops_airflow_plugin.plugin:LithopsAirflowPlugin'
        ]
    }
)
