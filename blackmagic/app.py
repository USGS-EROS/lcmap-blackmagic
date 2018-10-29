#!/usr/bin/env python3

from blackmagic import db
from cassandra import ConsistencyLevel
from cytoolz import count
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import partial
from cytoolz import second
from cytoolz import take
from datetime import date
from datetime import datetime
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing import Manager


import ccd
import json
import logging
import merlin
import os
import sys
import tornado.ioloop
import tornado.web


cfg = {'cassandra_host': os.environ['CASSANDRA_HOST'],
       'cassandra_port': int(os.environ.get('CASSANDRA_PORT', 9042)),
       'cassandra_user': os.environ.get('CASSANDRA_USER', 'cassandra'),
       'cassandra_pass': os.environ.get('CASSANDRA_PASS', 'cassandra'),
       'cassandra_keyspace': os.environ['CASSANDRA_KEYSPACE'],
       'cassandra_timeout': float(os.environ.get('CASSANDRA_TIMEOUT', 600)),
       'cassandra_consistency': ConsistencyLevel.name_to_value[os.environ.get('CASSANDRA_CONSISTENCY', 'QUORUM')],
       'cassandra_concurrent_writes': int(os.environ.get('CASSANDRA_CONCURRENT_WRITES', 1)),
       'chipmunk_url': os.environ['CHIPMUNK_URL'],
       'http_port': int(os.environ.get('HTTP_PORT', 5000)),
       'log_level': logging.INFO,
       'workers': int(os.environ.get('WORKERS', 1))}

logging.basicConfig(format='%(asctime)-15s %(name)-15s %(levelname)-8s - %(message)s', level=cfg['log_level'])
logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
logging.getLogger('cassandra.cluster').setLevel(logging.ERROR)
logger = logging.getLogger('blackmagic.app')

_workers = None
_writers = None
_q = None


def start_workers():
    global _workers
    if _workers is None:
        _workers = Pool(cfg['workers'])
    return _workers


def stop_workers():
    global _workers
    if _workers:
        _workers.close()
        _workers.join()
        _workers = None
    return _workers
    

def start_writers(q):
    global _writers
    if _writers is None:
        _writers = [Process(name='cassandra-writer[{}]'.format(i),
                            target=db.writer,
                            kwargs={'cfg': cfg, 'q': q},
                            daemon=True) for i in range(cfg['cassandra_concurrent_writes'])]
        [w.start() for w in _writers]
    return _writers

def stop_writers():
    global _writers
    if _writers:
        [w.join() for w in _writers]
        [w.terminate() for w in _writers]
    return _writers


def saveq():
    global _q
    if _q is None:
        _q = Manager().Queue()
    return _q


def saveccd(detection, q):
    q.put(db.insert_chip(cfg, detection))
    q.put(db.insert_pixel(cfg, detection))
    q.put(db.insert_segment(cfg, detection))
    return detection
    

def savepredictions(preds):
    pass


def default(change_models):
    # if there are no change models, append an empty one to
    # signify that ccd was run for the point, setting start_day and end_day to day 1

    return [{'start_day': 1, 'end_day': 1, 'break_day': 1}] if not change_models else change_models

    
def format(cx, cy, px, py, dates, ccdresult):
    logger.debug('formatting {},{},{},{}'.format(cx, cy, px, py))
    return [
             {'cx'     : int(cx),
              'cy'     : int(cy),
              'px'     : int(px),
              'py'     : int(py),
              'sday'   : date.fromordinal(get('start_day', cm)).isoformat(),
              'eday'   : date.fromordinal(get('end_day', cm)).isoformat(),
              'bday'   : date.fromordinal(get('break_day', cm, None)).isoformat(),
              'chprob' : get('change_probability', cm, None),
              'curqa'  : get('curve_qa', cm, None),
              'blmag'  : get_in(['blue', 'magnitude'], cm, None),
              'grmag'  : get_in(['green', 'magnitude'], cm, None),
              'remag'  : get_in(['red', 'magnitude'], cm, None),
              'nimag'  : get_in(['nir', 'magnitude'], cm, None),
              's1mag'  : get_in(['swir1', 'magnitude'], cm, None),
              's2mag'  : get_in(['swir2', 'magnitude'], cm, None),
              'thmag'  : get_in(['thermal', 'magnitude'], cm, None),
              'blrmse' : get_in(['blue', 'rmse'], cm, None),
              'grrmse' : get_in(['green', 'rmse'], cm, None),
              'rermse' : get_in(['red', 'rmse'], cm, None),
              'nirmse' : get_in(['nir', 'rmse'], cm, None),
              's1rmse' : get_in(['swir1', 'rmse'], cm, None),
              's2rmse' : get_in(['swir2', 'rmse'], cm, None),
              'thrmse' : get_in(['thermal', 'rmse'], cm, None),
              'blcoef' : list(get_in(['blue', 'coefficients'], cm, None)),
              'grcoef' : list(get_in(['green', 'coefficients'], cm, None)),
              'recoef' : list(get_in(['red', 'coefficients'], cm, None)),
              'nicoef' : list(get_in(['nir', 'coefficients'], cm, None)),
              's1coef' : list(get_in(['swir1', 'coefficients'], cm, None)),
              's2coef' : list(get_in(['swir2', 'coefficients'], cm, None)),
              'thcoef' : list(get_in(['thermal', 'coefficients'], cm, None)),
              'blint'  : get_in(['blue', 'intercept'], cm, None),
              'grint'  : get_in(['green', 'intercept'], cm, None),
              'reint'  : get_in(['red', 'intercept'], cm, None),
              'niint'  : get_in(['nir', 'intercept'], cm, None),
              's1int'  : get_in(['swir1', 'intercept'], cm, None),
              's2int'  : get_in(['swir2', 'intercept'], cm, None),
              'thint'  : get_in(['thermal', 'intercept'], cm, None),
              'dates'  : [date.fromordinal(o).isoformat() for o in dates],
              'mask'   : get('processing_mask', ccdresult, None)}
             for cm in default(get('change_models', ccdresult, None))]


def detect(timeseries):
   
    cx, cy, px, py = first(timeseries)

    return format(cx=cx,
                  cy=cy,
                  px=px,
                  py=py,
                  dates=get('dates', second(timeseries)),
                  ccdresult=ccd.detect(**second(timeseries)))


def pipeline(x, q):
    return count(map(partial(saveccd, q=q), detect(x)))


def delete_detections(timeseries):
    cx, cy, _, _ = first(first(timeseries))
    try:
        x = int(cx)
        y = int(cy)
        _ = db.execute(cfg, db.delete_chip(cfg, x, y))
        _ = db.execute(cfg, db.delete_pixel(cfg, x, y))
        _ = db.execute(cfg, db.delete_segment(cfg, x, y))
        
    except Exception as e:
        logger.exception('Exception deleting partition for x:{cx} y:{cy}'.format(cx=x, cy=y))

    return timeseries


class Health(tornado.web.RequestHandler):

    def get(self):
        self.write({'status': 'healthy'})

        
class Prediction(tornado.web.RequestHandler):

    def post(self):
        self.write('prediction')
        

class Segment(tornado.web.RequestHandler):

    def post(self):

        r = json.loads(self.request.body)
        x = r['cx']
        y = r['cy']
        n = get('n', r, 10000)

        a='0001-01-01/{}'.format(date.today().isoformat())
    
        logger.info('POST /segment {x},{y}'.format(x=x, y=y))

        timeseries = partial(merlin.create,
                             x=x,
                             y=y,
                             acquired=a,
                             cfg=merlin.cfg.get(profile='chipmunk-ard',
                                                env={'CHIPMUNK_URL': cfg['chipmunk_url']}))
    
        _workers.map(partial(pipeline, q=_q), take(n, delete_detections(timeseries())))
                    
        self.write({'cx': x, 'cy': y})
        

def startup():
    
    d = {'cfg': db.setup(cfg),
         'workers': start_workers(),
         'writers': start_writers(saveq())}
    
    logger.info('startup: cassandra-writers started?:{}'.format([w.is_alive() for w in d['writers']]))

    return d   

    
def shutdown():
    tornado.ioloop.IOLoop.instance().stop()
    return {'workers': stop_workers(),
            'writers': stop_writers()}
    
        
def application():

    return tornado.web.Application([
        (r'/segment', Segment),
        (r'/health', Health),
        (r'/prediction', Prediction)])

    
def main():
    logger.info('startup: configuration:{}'.format(cfg))

    try:
        startup()
        a = application()
        a.listen(cfg['http_port'])
        tornado.ioloop.IOLoop.current().start()
        
    except KeyboardInterrupt:
        shutdown()
    finally:
        shutdown()

        
if __name__ == '__main__':
    main()
