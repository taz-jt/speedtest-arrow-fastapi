import logging
from store import store

import json
import pyarrow.parquet as pq
#import pandas as pd
import pyarrow as pa
import numpy as np

import pyarrow.compute as pc

import time

class fetch(object):

    def __init__(self, logger, datasets_names = {}, load_method = 'memmap', maxAllowedReturnRows = 1e9, occ_threshold = 0.01):
        self.logging = logger
        self.datasets = {}
        self.maxAllowedReturnRows = maxAllowedReturnRows
        self.occ_threshold = occ_threshold
        self.load_method = load_method
        self.datasets_names = datasets_names

    # (!) if run on ray serve - need to check and load separately for each node
    def check_data_load(self):
        if len(self.datasets) == 0:
            if self.load_method == 'memmap':
                self.datasets = store.mmap_datasets_arrow(self.logging, self.datasets_names)

    def single(self,org_nr):

        self.check_data_load()

        table_basic = self.datasets['empl_basic']
        table_activity = self.datasets['empl_activity']
        table_stats = self.datasets['empl_stats']

        mask = (table_basic['organization_number'].to_numpy() == org_nr)

        t_basic = table_basic.filter(mask)
        t_act = table_activity.filter(mask)
        t_stats = table_stats.filter(mask)

        full_res = {}
        full_res.update(t_basic.to_pydict())
        full_res.update(t_act.to_pydict())
        full_res.update(t_stats.to_pydict())

        self.logging.info(t_basic.to_pydict())

        return full_res

    def occupation(self,occ_id):
        
        self.check_data_load()

        table_occupations = self.datasets['empl_occupations']

        if str(occ_id) not in table_occupations.column_names:
            return {'msg':'error: ' + str(occ_id) + ' not found' }

        mask = (table_occupations[str(occ_id)].to_numpy() > self.occ_threshold) # (!) compare timing to arrow compute

        table_basic = self.datasets['empl_basic']
        t_basic = table_basic.filter(mask)

        return t_basic.to_pydict()

    def speed_measure_sort(self, method = 'arrow'):

        self.check_data_load()

        self.logging.info('speed measure sort')

        t0 = time.process_time()
        table_basic = self.datasets['empl_basic']
        table_activity = self.datasets['empl_activity']
        table_stats = self.datasets['empl_stats']

        # alt numpy
        #table_sorted = np.sort(table_basic['organization_number'].to_numpy())

        # alt arrow
        indices = pc.call_function("sort_indices", [table_stats['rank12']])
        res = table_stats['organization_number'].take(indices)
        res_rank12 = table_stats['rank12'].take(indices)

        elapsed = round(time.process_time()*1000 - t0*1000)

        self.logging.info('elapsed: %s', elapsed)

        return res_rank12