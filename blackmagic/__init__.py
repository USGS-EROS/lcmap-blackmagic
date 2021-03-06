from cytoolz import first
from cytoolz import get
from functools import wraps
from multiprocessing import Pool

import logging
import os

cfg = {'ard_url': os.environ['ARD_URL'],
       'aux_url': os.environ['AUX_URL'],
       'log_level': logging.INFO,
       'cpus_per_worker': int(os.environ.get('CPUS_PER_WORKER', 1)),
       'xgboost': {'num_round': int(os.environ.get('XGBOOST_NUM_ROUND', 500)),
                   'test_size': float(os.environ.get('XGBOOST_TEST_SIZE', 0.2)),
                   'early_stopping_rounds': int(os.environ.get('XGBOOST_EARLY_STOPPING_ROUNDS', 10)),
                   'verbose_eval': False,
                   'target_samples': int(os.environ.get('XGBOOST_TARGET_SAMPLES', 20000000)),
                   'class_max': int(os.environ.get('XGBOOST_CLASS_MAX', 8000000)),
                   'class_min': int(os.environ.get('XGBOOST_CLASS_MIN', 600000)),
                   'parameters': {'objective': 'multi:softprob',
                                  'num_class': 9,
                                  'max_depth': 8,
                                  'tree_method': 'hist',
                                  'eval_metric': 'mlogloss',
                                  'silent': 1,
                                  'nthread': int(os.environ.get('CPUS_PER_WORKER', 1))}}}


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


def skip_on_empty(name):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            v = get(name, first(args), None)
            if v is None or (hasattr(v, '__len__') and len(v) == 0):
                return first(args)
            else:
                return fn(*args, **kwargs)
        return wrapper
    return decorator


def raise_on(name):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            
            if get(name, first(args), None) is not None:
                raise Exception(name)
            else:
                return fn(*args, **kwargs)
        return wrapper
    return decorator
