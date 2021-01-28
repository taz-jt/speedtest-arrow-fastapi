import pyarrow.parquet as pq
#import pandas as pd
import pyarrow as pa

import logging

# (!) changes:
# - switch to directories or pyarrow.dataset
# - add metadata descriptions

def mmap_datasets_arrow(logger = None, datasets_names = {}):

    if logger == None:
        logger = logging

    datasets = {}
    datasets_descriptions = {}

    for key in datasets_names:
        
        source = pa.memory_map(datasets_names[key], 'r')
        table = pa.ipc.RecordBatchFileReader(source).read_all()

        logger.info('memory-mapped %s', datasets_names[key])
        logger.info('size: cols = %s, rows = %s',
                         table.num_columns, table.num_rows)
        datasets[key] = table

    return datasets
