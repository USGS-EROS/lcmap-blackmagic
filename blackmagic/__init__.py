from cassandra import ConsistencyLevel
from cytoolz import first
from cytoolz import get
from functools import wraps
from multiprocessing import Pool

import logging
import os

cfg = {'cassandra_batch_size': int(os.environ.get('CASSANDRA_BATCH_SIZE', 1000)),
       'cassandra_host': str(os.environ['CASSANDRA_HOST']).split(','),
       'cassandra_port': int(os.environ.get('CASSANDRA_PORT', 9042)),
       'cassandra_user': os.environ.get('CASSANDRA_USER', 'cassandra'),
       'cassandra_pass': os.environ.get('CASSANDRA_PASS', 'cassandra'),
       'cassandra_keyspace': os.environ['CASSANDRA_KEYSPACE'],
       'cassandra_timeout': float(os.environ.get('CASSANDRA_TIMEOUT', 600)),
       'cassandra_consistency': ConsistencyLevel.name_to_value[os.environ.get('CASSANDRA_CONSISTENCY', 'ALL')],
       'cassandra_concurrent_writes': int(os.environ.get('CASSANDRA_CONCURRENT_WRITES', 1)),
       'ard_url': os.environ['ARD_URL'],
       'aux_url': os.environ['AUX_URL'],
       'log_level': logging.INFO,
       'cpus_per_worker': int(os.environ.get('CPUS_PER_WORKER', 1)),
       'xgboost': {'num_round': 500,
                   'test_size': 0.2,
                   'early_stopping_rounds': 10,
                   'verbose_eval': False,
                   'parameters': {'objective': 'multi:softprob',
                                  'num_class': 9,
                                  'max_depth': 8,
                                  'tree_method': 'hist',
                                  'eval_metric': 'mlogloss',
                                  'silent': 1,
                                  'nthread': -1}}}


def workers(cfg):
    return Pool(cfg['cpus_per_worker'])


def skip_on_exception(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        
        if get('exception', first(args), None) is None:
            return fn(*args, **kwargs)
        else:
            return first(args)
        
    return wrapper
