FROM python:3.10.1

RUN apt update \
    && apt -y install libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
RUN mkdir databaser tmp
COPY ./src /srv/databaser

COPY requirements.txt /srv/tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /srv/tmp/requirements.txt

CMD python3 /srv/databaser/manage.py