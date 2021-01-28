FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

ARG ARGDIR=/app/data/
#ARG ARGDIR=/mnt/vol
ENV FILEDIR=$ARGDIR

RUN mkdir $ARGDIR

WORKDIR /app

COPY ./app /app

# Replace to point at HTTPS files
# ? or copy with oc to mnt instead

ADD data/empl_activity.arrow $ARGDIR
ADD data/empl_basic.arrow $ARGDIR
ADD data/empl_occupations.arrow $ARGDIR
ADD data/empl_stats.arrow $ARGDIR

#ADD https://file1 DIRFILES
#ADD https://file1 DIRFILES

COPY requirements.txt /app

RUN pip install -r requirements.txt