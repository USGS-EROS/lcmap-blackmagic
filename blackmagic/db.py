from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.policies import RetryPolicy
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement
from cassandra.query import BatchType
from cytoolz import first
from cytoolz import partition_all

import cassandra
import logging

logger = logging.getLogger(__name__)


def cluster(cfg, keyspace=None):
    auth = PlainTextAuthProvider(username=cfg['cassandra_user'],
                                 password=cfg['cassandra_pass'])

    cluster = Cluster(cfg['cassandra_host'],
                      load_balancing_policy=RoundRobinPolicy(),
                      default_retry_policy=RetryPolicy(),
                      reconnection_policy=ExponentialReconnectionPolicy(1.0, 600.0),
                      port=cfg['cassandra_port'],
                      auth_provider=auth)
    return cluster


def session(cfg, cluster, keyspace=None):
    session = cluster.connect(keyspace=keyspace)
    session.default_timeout = cfg['cassandra_timeout']
    session.default_fetch_size = None
    return session


def none_as_null(s):
    return s.replace('None', 'null')


def execute_statement(cfg, stmt, keyspace=None):
    s = None
    try:
        s = session(cfg, cluster(cfg, keyspace), keyspace)
        return s.execute(none_as_null(stmt))
    finally:
        if s:
            s.shutdown()
                
                           
def execute_statements(cfg, stmts, keyspace=None):
    s = None
    try:
        s = session(cfg, cluster(cfg, keyspace), keyspace)
        return [s.execute(none_as_null(st)) for st in stmts]
    
    finally:
        if s:
            s.shutdown()
        

def create_keyspace(cfg):
    s = '''CREATE KEYSPACE IF NOT EXISTS {keyspace} 
           WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 
                                 'replication_factor' : 1}};'''

    return s.format(keyspace=cfg['cassandra_keyspace'])


def create_tile(cfg):
    '''
        tx:    tile upper left x
        ty:    tile upper left y
        model: model text
    '''
    s = '''CREATE TABLE IF NOT EXISTS {keyspace}.tile (
           tx      int,
           ty      int,
           model   text,
           PRIMARY KEY((tx, ty)))
           WITH COMPRESSION = {{ 'sstable_compression': 'LZ4Compressor' }}
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }}
           AND  GC_GRACE_SECONDS = 172800;'''

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
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }}
           AND  GC_GRACE_SECONDS = 172800;'''

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
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }}
           AND  GC_GRACE_SECONDS = 172800;'''
    
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
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }}
           AND  GC_GRACE_SECONDS = 172800;'''

    return s.format(keyspace=cfg['cassandra_keyspace'])


def create_prediction(cfg):
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
    s = '''CREATE TABLE IF NOT EXISTS {keyspace}.prediction (
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
           AND  COMPACTION  = {{ 'class': 'LeveledCompactionStrategy' }}
           AND  GC_GRACE_SECONDS = 172800;'''

    return s.format(keyspace=cfg['cassandra_keyspace'])


def setup(cfg):
    logger.info('setting up database')
    try:
        s = [create_keyspace(cfg),
             create_tile(cfg),
             create_chip(cfg),
             create_pixel(cfg),
             create_segment(cfg),
             create_prediction(cfg)]
        execute_statements(cfg, s)
        
    except Exception as e:
        logger.exception('setup exception:{}'.format(e))

    return cfg


def insert_chips(cfg, detections):
    s = session(cfg, cluster(cfg))
    detection = first(detections)
    try:
        st = 'INSERT INTO {keyspace}.chip (cx, cy, dates) VALUES (?, ?, ?)'.format(keyspace=cfg['cassandra_keyspace'])
        stmt = s.prepare(st)
        return s.execute(stmt, [detection['cx'],
                                detection['cy'],
                                detection['dates']])
    finally:
        if s:
            s.shutdown()
        

def insert_pixels(cfg, detections):
    s = session(cfg, cluster(cfg))
    try:
        st = 'INSERT INTO {keyspace}.pixel (cx, cy, px, py, mask) VALUES (?, ?, ?, ?, ?)'.format(keyspace=cfg['cassandra_keyspace'])
        stmt = s.prepare(st)

        chunks = partition_all(cfg['cassandra_batch_size'], detections)

        batches = []

        for chunk in chunks:
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            [batch.add(stmt, [c['cx'], c['cy'], c['px'], c['py'], c['mask']]) for c in chunk]
            batches.append(batch)
           
        return [s.execute(b) for b in batches]
    finally:
        if s:
            s.shutdown()

            
def insert_tile(cfg, tx, ty, model):
    s = session(cfg, cluster(cfg))

    try:
        st = 'INSERT INTO {keyspace}.tile (tx, ty, model) VALUES (%(tx)s, %(ty)s, %(model)s);'
        st = st.format(keyspace=cfg['cassandra_keyspace'])

        p = {"tx":    tx,
             "ty":    ty,
             "model": model}

        return s.execute(none_as_null(st), p)
    finally:
        if s:
            s.shutdown()
              
                
def insert_segments(cfg, detections):
    s = session(cfg, cluster(cfg))
    
    try:
        st = '''INSERT INTO {keyspace}.segment 
                    (cx, cy, px, py, sday, eday, bday, chprob, curqa,
                     blcoef, blint, blmag, blrmse,
                     grcoef, grint, grmag, grrmse,
                     nicoef, niint, nimag, nirmse,
                     recoef, reint, remag, rermse,
                     s1coef, s1int, s1mag, s1rmse,
                     s2coef, s2int, s2mag, s2rmse,
                     thcoef, thint, thmag, thrmse) 
                VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?, ?)'''.format(keyspace=cfg['cassandra_keyspace'])
        
        stmt = s.prepare(st)

        chunks = partition_all(cfg['cassandra_batch_size'], detections)
        
        batches = []

        for chunk in chunks:
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)

            for c in chunk:
                batch.add(stmt, [c['cx'], c['cy'], c['px'], c['py'], c['sday'], c['eday'], c['bday'], c['chprob'], c['curqa'],
                                 c['blcoef'], c['blint'], c['blmag'], c['blrmse'],
                                 c['grcoef'], c['grint'], c['grmag'], c['grrmse'],
                                 c['nicoef'], c['niint'], c['nimag'], c['nirmse'],
                                 c['recoef'], c['reint'], c['remag'], c['rermse'],
                                 c['s1coef'], c['s1int'], c['s1mag'], c['s1rmse'],
                                 c['s2coef'], c['s2int'], c['s2mag'], c['s2rmse'],
                                 c['thcoef'], c['thint'], c['thmag'], c['thrmse']])
            batches.append(batch)
           
        return [s.execute(b) for b in batches]

    finally:
        if s:
            s.shutdown()

            
def insert_predictions(cfg, predictions):
    s = session(cfg, cluster(cfg))
    
    try:
        st = '''INSERT INTO {keyspace}.prediction 
                    (cx, cy, px, py, sday, eday, date, prob) 
                VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?)'''.format(keyspace=cfg['cassandra_keyspace'])
        
        stmt = s.prepare(st)

        chunks = partition_all(cfg['cassandra_batch_size'], predictions)
        
        batches = []

        for chunk in chunks:
                        
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)

            for c in chunk:
                
                batch.add(stmt, [c['cx'], c['cy'], c['px'], c['py'], c['sday'], c['eday'], c['date'], c['prob']])
            batches.append(batch)
           
        return [s.execute(b) for b in batches]

    finally:
        if s:
            s.shutdown()
            

def delete_chip(cfg, cx, cy):
    s = 'DELETE FROM {keyspace}.chip WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def delete_pixels(cfg, cx, cy):
    s = 'DELETE FROM {keyspace}.pixel WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def delete_segments(cfg, cx, cy):
    s = 'DELETE FROM {keyspace}.segment WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def delete_tile(cfg, tx, ty):
    s = 'DELETE FROM {keyspace}.tile WHERE tx={tx} AND ty={ty};'
    return s.format(keyspace=cfg['cassandra_keyspace'], tx=tx, ty=ty)


def delete_predictions(cfg, cx, cy):
    s = 'DELETE FROM {keyspace}.prediction WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def select_chip(cfg, cx, cy):
    s = 'SELECT * FROM {keyspace}.chip WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def select_pixels(cfg, cx, cy):
    s = 'SELECT * FROM {keyspace}.pixel WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def select_segments(cfg, cx, cy):
    s = 'SELECT * FROM {keyspace}.segment WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)


def select_tile(cfg, tx, ty):
    s = 'SELECT * FROM {keyspace}.tile WHERE tx={tx} AND ty={ty};'
    return s.format(keyspace=cfg['cassandra_keyspace'], tx=tx, ty=ty)

def select_predictions(cfg, cx, cy):
    s = 'SELECT * FROM {keyspace}.prediction WHERE cx={cx} AND cy={cy};'
    return s.format(keyspace=cfg['cassandra_keyspace'], cx=cx, cy=cy)
