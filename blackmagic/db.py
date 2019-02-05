from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

import cassandra
import logging

logger = logging.getLogger(__name__)


def none_as_null(s):
    return s.replace('None', 'null')


def connect(cfg, keyspace=None):
    auth = PlainTextAuthProvider(username=cfg['cassandra_user'],
                                 password=cfg['cassandra_pass'])

    cluster = Cluster([cfg['cassandra_host'],],
                      load_balancing_policy=RoundRobinPolicy(),
                      port=cfg['cassandra_port'],
                      auth_provider=auth)
    
    session = cluster.connect(keyspace=keyspace)
    session.default_timeout = cfg['cassandra_timeout']
    session.default_fetch_size = None

    return {'cluster': cluster, 'session': session}


def execute(cfg, stmt, keyspace=None):
    conn = None
    try:
        s = none_as_null(stmt)
        conn = connect(cfg, keyspace)
        return conn['session'].execute(s)
    except Exception as e:
        logger.error('statement:{}'.format(s))
        logger.exception('db execution exception:{}'.format(e))
    finally:
        if conn:
            if conn['session']:
                conn['session'].shutdown()
            if conn['cluster']:
                conn['cluster'].shutdown()

                
def writer(cfg, q, errorq):

    conn = None
    
    try:
        conn = connect(cfg)
    
        while True:
            stmt = q.get()
            if stmt == 'STOP_WRITER':
                logger.debug('stopping writer')
                break
            logger.debug('writing:{}'.format(none_as_null(stmt)))
            try:
                rows=conn['session'].execute(none_as_null(stmt))
            except Exception as e:
                msg = 'statement:{}'.format(non_as_null(stmt))
                errorq.put('db execution error: {}'.format(msg))
                logger.error(msg)
                logger.exception('db execution error')
                continue
    except:
        errorq.put('db connection error')
        logger.exception('db connection error')
    finally:
        if conn:
            if conn['session']:
                conn['session'].shutdown()
            if conn['cluster']:
                conn['cluster'].shutdown()


def create_keyspace(cfg):
    s = '''CREATE KEYSPACE IF NOT EXISTS {keyspace} 
           WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 
                                 'replication_factor' : 1}};'''

    return s.format(keyspace=cfg['cassandra_keyspace'])


def create_tile(cfg):
    '''
        tx:    tile upper left x
        ty:    tile upper left y
        model: model blob
    '''
    s = '''CREATE TABLE IF NOT EXISTS {keyspace}.tile (
           tx      int,
           ty      int,
           model   blob,
           PRIMARY KEY((tx, ty)))
           WITH COMPRESSION = {{ 'sstable_compression': 'LZ4Compressor' }}
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }};'''

    return s.format(keyspace=cfg['cassandra_keyspace'])


def create_chip(cfg):
    '''
        cx: upper left x of the chip
        cy: upper left y of the chip
        dates: ARD timestamps for this chip
    '''
    s = '''CREATE TABLE IF NOT EXISTS {keyspace}.chip (
           cx    int,
           cy    int,
           dates frozen<list<text>>,
           PRIMARY KEY((cx, cy)))
           WITH COMPRESSION = {{ 'sstable_compression': 'LZ4Compressor' }}
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }};'''

    return s.format(keyspace=cfg['cassandra_keyspace'])
    


def create_pixel(cfg):
    ''' 
        cx: upper left x of the chip
        cy: upper left y of the chip
        px: x pixel coordinate
        py: y pixel coordinate
        mask: processing mask, 0/1 for not used/used in calculation
       (applies against dates)

    '''
    s = '''CREATE TABLE IF NOT EXISTS {keyspace}.pixel (
           cx   int,
           cy   int,
           px   int,
           py   int,
           mask frozen<list<tinyint>>,,
           PRIMARY KEY((cx, cy), px, py))
           WITH COMPRESSION = {{ 'sstable_compression': 'LZ4Compressor' }}
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }};'''
    
    return s.format(keyspace=cfg['cassandra_keyspace'])


def create_segment(cfg):
    '''
        If sday and eday are 0, there was no change detected for that location

        cx:     upper left x of the chip
        cy:     upper left y of the chip
        px:     x pixel coordinate
        py:     y pixel coordinate
        sday:   start date
        eday:   end date
        bday:   break date
        chprob: change_probability
        curqa:  curve_qa
        blmag:  blue magnitude
        grmag:  green magnitude
        remag:  red magnitude
        nimag:  nir magnitude
        s1mag:  swir1 magnitude
        s2mag:  swir2 magnitude
        thmag:  thermal magnitude
        blrmse: blue rmse
        grrmse: green rmse
        rermse: red rmse
        nirmse: nir rmse
        s1rmse: swir1 rmse
        s2rmse: swir2 rmse
        thrmse: thermal rmse
        blcoef: blue coefficients
        grcoef: green coefficients
        recoef: red coefficients
        nicoef: nir coefficients
        s1coef: swir1 coefficients
        s2coef: swir2 coefficients
        thcoef: thermal coefficients
        blint:  blue intercept
        grint:  green intercept
        reint:  red intercept
        niint:  nir intercept
        s1int:  swir1 intercept
        s2int:  swir2 intercept
        thint:  thermal intercept
'''
    s = '''CREATE TABLE IF NOT EXISTS {keyspace}.segment (
           cx     int,
           cy     int,
           px     int,
           py     int,
           sday   text,
           eday   text,
           bday   text,
           chprob float,
           curqa  tinyint,
           blmag  float,
           grmag  float,
           remag  float,
           nimag  float,
           s1mag  float,
           s2mag  float,
           thmag  float,
           blrmse float,
           grrmse float,
           rermse float,
           nirmse float,
           s1rmse float,
           s2rmse float,
           thrmse float,
           blcoef frozen<list<float>>,
           grcoef frozen<list<float>>,
           recoef frozen<list<float>>,
           nicoef frozen<list<float>>,
           s1coef frozen<list<float>>,
           s2coef frozen<list<float>>,
           thcoef frozen<list<float>>,
           blint  float,
           grint  float,
           reint  float,
           niint  float,
           s1int  float,
           s2int  float,
           thint  float,    
           PRIMARY KEY((cx, cy), px, py, sday, eday))     
           WITH COMPRESSION = {{ 'sstable_compression': 'LZ4Compressor' }}
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }};'''

    return s.format(keyspace=cfg['cassandra_keyspace'])


def create_annual_prediction(cfg):
    '''
        cx:   upper left x of the chip
        cy:   upper left y of the chip
        px:   x pixel coordinate
        py:   y pixel coordinate
        sday: start date
        eday: end date
        date: prediction date
        prob: xgboost classification probabilities
    '''
    s = '''CREATE TABLE IF NOT EXISTS {keyspace}.annual_prediction (
           cx   int,
           cy   int,
           px   int,
           py   int,
           sday text,
           eday text,
           date text,
           prob frozen<list<float>>,
           PRIMARY KEY((cx, cy), px, py, sday, eday, date))
           WITH COMPRESSION = {{ 'sstable_compression': 'LZ4Compressor' }}
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }};'''

    return s.format(keyspace=cfg['cassandra_keyspace'])


def setup(cfg):
    logger.info('setting up database')
    try:
        execute(cfg, create_keyspace(cfg), None)
        execute(cfg, create_tile(cfg))
        execute(cfg, create_chip(cfg))
        execute(cfg, create_pixel(cfg))
        execute(cfg, create_segment(cfg))
        execute(cfg, create_annual_prediction(cfg))
    except Exception as e:
        logger.exception('setup exception:{}'.format(e))

    return cfg


def insert_tile(cfg, tx, ty, model):
    s = 'INSERT INTO {keyspace}.tile (tx, ty, model) VALUES ({tx}, {ty}, {model});'

    return s.format(keyspace=cfg['cassandra_keyspace'],
                    tx=tx,
                    ty=ty,
                    model=model.hex())


def insert_chip(cfg, detection):
    s = 'INSERT INTO {keyspace}.chip (cx, cy, dates) VALUES ({cx}, {cy}, {dates});'

    return s.format(keyspace=cfg['cassandra_keyspace'],
                    cx=detection['cx'],
                    cy=detection['cy'],
                    dates=detection['dates'])


def insert_pixel(cfg, detection):
    s = 'INSERT INTO {keyspace}.pixel (cx, cy, px, py, mask) VALUES ({cx}, {cy}, {px}, {py}, {mask});'

    return s.format(keyspace=cfg['cassandra_keyspace'],
                    cx=detection['cx'],
                    cy=detection['cy'],
                    px=detection['px'],
                    py=detection['py'],
                    mask=detection['mask'])


def insert_segment(cfg, detection):
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


def delete_chip(cfg, cx, cy):
    s = 'DELETE FROM {keyspace}.chip WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def delete_pixel(cfg, cx, cy):
    s = 'DELETE FROM {keyspace}.pixel WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def delete_segment(cfg, cx, cy):
    s = 'DELETE FROM {keyspace}.segment WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def select_segment(cfg, cx, cy):
    s = 'SELECT * FROM {keyspace}.segment WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)
