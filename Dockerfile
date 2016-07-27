FROM ubuntu:14.04
RUN apt-get update && apt-get -y install python-setuptools build-essential python-dev libffi-dev libssl-dev
RUN easy_install pip
RUN pip install urllib3[secure]==1.15.1
RUN pip install boto==2.39.0
RUN pip install boto3==1.3.0
RUN pip install pytz
EXPOSE 5001
ADD . /src/
CMD ["python", "/src/main.py"]
