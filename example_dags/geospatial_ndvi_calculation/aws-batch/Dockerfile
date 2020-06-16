FROM python:2
ENV DEBIAN_FRONTEND noninteractive
ENV SEN2COR_URL http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run
COPY setup.sh /opt/
WORKDIR /opt
RUN chmod u+x setup.sh && ./setup.sh
ENV SEN2COR_COM /opt/Sen2Cor-02.08.00-Linux64/bin/L2A_Process
COPY /sen2cor /sen2cor/
WORKDIR /sen2cor
RUN python3 -m pip install -r requirements.txt
ENTRYPOINT [ "python3", "run.py" ]
# CMD ["sleep", "100000000"]
