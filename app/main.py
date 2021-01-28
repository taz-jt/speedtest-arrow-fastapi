from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import logging
import time
import os

from fetch import fetch

app = FastAPI()

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(filename='log1.log', format='%(asctime)s - %(message)s', level=logging.INFO)

file_dir = os.getenv('FILEDIR','/')

datasets_names = {
    'empl_basic': file_dir + 'empl_basic.arrow',
    'empl_stats': file_dir + 'empl_stats.arrow',
    'empl_activity': file_dir + 'empl_activity.arrow',
    'empl_occupations': file_dir + 'empl_occupations.arrow',
#    'empl_industry': file_dir + 'empl_industry.arrow'
#    'pb': file_dir + 'pb_2006_2020.arrow',
#    'pb_occupations': file_dir + 'pb_occupations.arrow',
    }

fetcher = fetch(logging, datasets_names)


def speed_test1(iterations):

    logging.info('speed test 1, %d iterations', iterations)

    t0 = time.process_time()
    org_nr = '2120000142'#5568021579
    occ_id = '1000'

    for i in range(iterations):
        res_single = fetcher.single(org_nr)

    elapsed = round(time.process_time()*1000 - t0*1000)
    logging.info('t1 = %d', elapsed)

    for i in range(iterations):
        res_occ = fetcher.occupation(occ_id)

    elapsed = round(time.process_time()*1000 - t0*1000)
    logging.info('t2 = %d', elapsed)

    for i in range(iterations):
        res_sort = fetcher.speed_measure_sort()

    elapsed = round(time.process_time()*1000 - t0*1000)
    logging.info('tfinal = %d', elapsed)

    return elapsed


@app.get("/")
def main():
    return { 'msg': 'ok' }


@app.get("/speed_test1/{iterations}")
def read_item(iterations: int, q: Optional[str] = None):

    logging.info('speed test 1 with %s iterations')

    elapsed = speed_test1(iterations)

    return {'msg':'ok', 'time [ms]': str(elapsed), 'description': '', 'orient': '', 'data': '' }


@app.get("/employer/{org_nr}")
def read_item(org_nr: str, q: Optional[str] = None):
    logging.info('req single empl orgnr: %s', org_nr)

    t0 = time.process_time()

    res = fetcher.single(org_nr)

    elapsed = round(time.process_time()*1000 - t0*1000)

    #logging.info(res)

    if res == None:
        return {'msg':'error, result None'}

    return {'msg':'ok', 'time [ms]': str(elapsed), 'description': '', 'orient': '', 'data': res }


@app.get("/occupation/{occ_id}/{region}")
def read_item(occ_id: str, q: Optional[str] = None):
    logging.info('req occupation id: %s', occ_id)

    t0 = time.process_time()

    res = fetcher.occupation(occ_id)

    elapsed = round(time.process_time()*1000 - t0*1000)

    #logging.info(res)

    if res == None:
        return {'msg':'error, result None'}

    return {'msg':'ok', 'time [ms]': str(elapsed), 'description': '', 'orient': '', 'data': res }


# manual speed tests on first load (first call will load data)
speed_test1(1)
speed_test1(1)
speed_test1(5)
speed_test1(10)