FROM ubuntu:20.04
RUN apt update
#RUN apt install software-properties-common
#RUN apt update
#RUN add-apt-repository ppa:pypy/ppa
#RUN apt update
RUN apt install -y pypy3 pypy3-dev wget g++

RUN wget https://bootstrap.pypa.io/pip/3.6/get-pip.py

RUN pypy3 get-pip.py

RUN pypy3 -mpip install Cython pandas

RUN pypy3 -mpip install pycryptodome

#WORKDIR /app

COPY . .

CMD [ "pypy3", "mainB.py", "-n4", "-ddatasetB.csv" ]

#CMD [ "pypy3", "mainB.py"]


