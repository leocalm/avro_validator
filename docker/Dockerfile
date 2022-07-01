FROM python:3

# OS Dependencies
RUN apt-get update && apt-get install -y locales \
                                         curl \
                                         wget \
                                         vim

RUN useradd -ms /bin/bash test

WORKDIR /code
ENV PYTHONPATH=/code

COPY requirements/test.txt /tmp/
RUN pip install -r /tmp/test.txt

CMD bash

USER test
