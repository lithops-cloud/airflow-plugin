import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cloudbutton_airflow_plugin",
    version="1.0.0",
    author="Aitor Arjona",
    author_email="aitor.arjona@urv.com",
    description="Cloudbutton plugin for Apache Airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cloudbutton/airflow-plugin",
#    install_requires=[
#        "pywren-ibm-cloud>=1.5.2 "
#    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    entry_points = {
        'airflow.plugins': [
            'plugin = cloudbutton_airflow_plugin.plugin:CloudbuttonAirflowPlugin'
        ]
    }
)
