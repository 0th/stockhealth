
FROM ubuntu:16.04

ENV LANG C.UTF-8

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get update
RUN apt-get install -y --no-install-recommends python3.6 python3.6-dev python3-pip python3-setuptools python3-wheel gcc
RUN apt-get install -y git
RUN python3.6 -m pip install pip --upgrade

ADD . /pyapp-test7
WORKDIR /pyapp-test7

RUN pip3 install -r requirements.txt

CMD python3.6 Main.py