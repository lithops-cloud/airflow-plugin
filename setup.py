import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pywren_ibm_cloud_airflow",
    version="1.1.1",
    author="Aitor Arjona",
    author_email="aitor.arjona@urv.com",
    description="IBM Cloud PyWren plugin for Apache Airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cloudbutton/ibm-pywren_airflow-plugin",
    install_requires=[
        "pywren-ibm-cloud>=1.5.2 "
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    entry_points = {
        'airflow.plugins': [
            'plugin = pywren_ibm_cloud_airflow.plugin:IBMPyWrenAirflowPlugin'
        ]
    }
)