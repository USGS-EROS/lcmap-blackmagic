#!/usr/bin/env python3

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cytoolz import count
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import partial
from cytoolz import second
from cytoolz import take
from datetime import date
from datetime import datetime
from flask import Flask
from flask import jsonify
from flask import request
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing import Manager

import cassandra
import ccd
import logging
import merlin
import os
import sys


app = Flask(__name__)
workers = None
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
saveq = None

logging.basicConfig(format='%(asctime)-15s %(module)-10s %(levelname)-8s - %(message)s',
                    level=cfg['log_level'])

logger = logging.getLogger(__name__)


def connection():
    auth = PlainTextAuthProvider(username=cfg['cassandra_user'],
                                 password=cfg['cassandra_pass'])

    cluster = Cluster([cfg['cassandra_host'],],
                      load_balancing_policy=RoundRobinPolicy(),
                      port=cfg['cassandra_port'],
                      auth_provider=auth)
    
    session = cluster.connect(keyspace=cfg['cassandra_keyspace'])
    session.default_timeout = cfg['cassandra_timeout']

    return {'cluster': cluster, 'session': session}

        
def insert_chip(detection):
    s = 'INSERT INTO {keyspace}.chip (cx, cy, dates) VALUES ({cx}, {cy}, {dates});'

    return s.format(keyspace=cfg['cassandra_keyspace'],
                    cx=detection['cx'],
                    cy=detection['cy'],
                    dates=detection['dates'])


def insert_pixel(detection):
    s = 'INSERT INTO {keyspace}.pixel (cx, cy, px, py, mask) VALUES ({cx}, {cy}, {px}, {py}, {mask});'

    return s.format(keyspace=cfg['cassandra_keyspace'],
                    cx=detection['cx'],
                    cy=detection['cy'],
                    px=detection['px'],
                    py=detection['py'],
                    mask=detection['mask'])


def insert_segment(detection):
    s =  '''INSERT INTO {keyspace}.segment 
                (cx, cy, px, py, sday, eday, bday, chprob, curqa,
                 blcoef, blint, blmag, blrmse,
                 grcoef, grint, grmag, grrmse,
                 nicoef, niint, nimag, nirmse,
                 recoef, reint, remag, rermse,
                 s1coef, s1int, s1mag, s1rmse,
                 s2coef, s2int, s2mag, s2rmse,
                 thcoef, thint, thmag, thrmse) 
            VALUES 
               ({cx}, {cy}, {px}, {py}, '{sday}', '{eday}', '{bday}', {chprob}, {curqa},
                {blcoef}, {blint}, {blmag}, {blrmse},
                {grcoef}, {grint}, {grmag}, {grrmse},
                {nicoef}, {niint}, {nimag}, {nirmse},
                {recoef}, {reint}, {remag}, {rermse},
                {s1coef}, {s1int}, {s1mag}, {s1rmse},
                {s2coef}, {s2int}, {s2mag}, {s2rmse},
                {thcoef}, {thint}, {thmag}, {thrmse});'''
    return s.format(keyspace=cfg['cassandra_keyspace'], **detection)


def saveccd(detection, q):
    q.put(insert_chip(detection))
    q.put(insert_pixel(detection))
    q.put(insert_segment(detection))
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


@app.route('/health', methods=['GET'])
def health():
    return jsonify(True)


def pipeline(x, q):
    return count(map(partial(saveccd, q=q), detect(x)))


def delete_segments(timeseries):
    cx, cy, _, _ = first(first(timeseries))
    conn = None
    try:
        x = int(cx)
        y = int(cy)
        conn = connection()
        rows = conn['session'].execute('DELETE FROM {keyspace}.chip WHERE cx={cx} AND cy={cy};'.format(keyspace=cfg['cassandra_keyspace'], cx=x, cy=y))
        rows = conn['session'].execute('DELETE FROM {keyspace}.pixel WHERE cx={cx} AND cy={cy};'.format(keyspace=cfg['cassandra_keyspace'], cx=x, cy=y))
        rows = conn['session'].execute('DELETE FROM {keyspace}.segment WHERE cx={cx} AND cy={cy};'.format(keyspace=cfg['cassandra_keyspace'], cx=x, cy=y))
    except Exception as e:
        logger.exception('Exception deleting partition for x:{cx} y:{cy}'.format(cx=x, cy=y))
    finally:
        if conn:
            if conn['session']:
                conn['session'].shutdown()
            if conn['cluster']:
                conn['cluster'].shutdown()
    return timeseries
                

@app.route('/segment', methods=['POST'])
def segment():
    
    r = request.json
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
    
    workers.map(partial(pipeline, q=saveq), take(n, delete_segments(timeseries())))
    
    return jsonify({'cx':x, 'cy':y})
        
               
@app.route('/prediction', methods=['POST'])
def prediction():

    r = request.json
    # generate some predictions
    return True


def cassandra_writer(q):

    db = None
    
    try:
        db = connection()
    
        while True:
            stmt = q.get()
            logger.debug('writing:{}'.format(stmt))
            try:
                rows=db['session'].execute(stmt)
            except Exception as e:
                logger.exception('db execution error')
                continue
    except:
        logger.exception('db connection error')
    finally:
        if db:
            if db['session']:
                db['session'].shutdown()
            if db['cluster']:
                db['cluster'].shutdown()
                

def main():
    
    logger.info('startup: configuration:{}'.format(cfg))
    global workers    
    workers = Pool(cfg['workers'])
    saveq = Manager().Queue()

    writers = [Process(name='cassandra-writer[{}]'.format(i),
                       target=cassandra_writer,
                       kwargs={'q': saveq},
                       daemon=True) for i in range(cfg['cassandra_concurrent_writes'])]
    [w.start() for w in writers]

    logger.info('startup: cassandra-writers started?:{}'.format([w.is_alive() for w in writers]))
          
    try:
        app.run(use_reloader=False, port=cfg['http_port'])
    except KeyboardInterrupt:
        pass
    finally:
        workers.close()
        workers.join()
        [w.join() for w in writers]
        [w.terminate() for w in writers]

        
if __name__ == '__main__':
    main()
