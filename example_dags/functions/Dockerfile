FROM ibmfunctions/action-python-v3.6

RUN pip3 install matplotlib
RUN apt-get update && \
	apt-get install libgeos-dev -y && \
	pip3 install --user https://github.com/matplotlib/basemap/archive/master.zip
