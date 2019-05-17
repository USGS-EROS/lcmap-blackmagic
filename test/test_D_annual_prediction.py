import json
import os
import pytest
import random
import test

from blackmagic import app
from blackmagic import db
from cassandra.cluster import Cluster
from cytoolz import get
from cytoolz import merge
from cytoolz import reduce

from datetime import date


def delete_annual_predictions(cx, cy):
    return db.execute_statements(app.cfg,
                                 [db.delete_annual_predictions(app.cfg, cx, cy)])


@pytest.fixture
def client():
    app.app.config['TESTING'] = True
    yield app.app.test_client()


def annual_prediction_test_data(segment):
    return merge(segment,
                 {'sday': date.fromordinal(2).isoformat(),
                  'eday': date.fromordinal(20000).isoformat(),
                  'blcoef': [random.uniform(0, 1) for i in range(7)],
                  'blint':  random.randint(0, 90),
                  'blmag':  random.randint(0, 10),
                  'blrmse': random.random(),
                  'grcoef': [random.uniform(0, 1) for i in range(7)],
                  'grint':  random.randint(0, 90),
                  'grmag':  random.randint(0, 10),
                  'grrmse': random.random(),
                  'nicoef': [random.uniform(0, 1) for i in range(7)],
                  'niint':  random.randint(0, 90),
                  'nimag':  random.randint(0, 10),
                  'nirmse': random.random(),
                  'recoef': [random.uniform(0, 1) for i in range(7)],
                  'reint':  random.randint(0, 90),
                  'remag':  random.randint(0, 10),
                  'rermse': random.random(),                     
                  's1coef': [random.uniform(0, 1) for i in range(7)],
                  's1int':  random.randint(0, 90),
                  's1mag':  random.randint(0, 10),
                  's1rmse': random.random(),
                  's2coef': [random.uniform(0, 1) for i in range(7)],
                  's2int':  random.randint(0, 90),
                  's2mag':  random.randint(0, 10),
                  's2rmse': random.random(),
                  'thcoef': [random.uniform(0, 1) for i in range(7)],
                  'thint':  random.randint(0, 90),
                  'thmag':  random.randint(0, 10),
                  'thrmse': random.random()})
    

def create_annual_prediction_test_data(client):

    # prepopulate a chip of segments
    assert client.post('/segment',
                       json={'cx': test.cx,
                             'cy': test.cy,
                             'acquired': test.acquired}).status == '200 OK'

    # pull the segments
    segments = db.execute_statement(app.cfg,
                                    db.select_segments(cfg=app.cfg,
                                                       cx=test.cx,
                                                       cy=test.cy))

    # remove old and busted test data
    db.execute_statement(app.cfg,
                         db.delete_segments(cfg=app.cfg,
                                            cx=test.cx,
                                            cy=test.cy))

    # add new and better test data
    db.insert_segments(app.cfg,
                       map(annual_prediction_test_data,
                           map(lambda x: x._asdict(), segments)))      
               
    # train tile against new segment values
    assert client.post('/tile',
                       json={'tx': test.tx,
                             'ty': test.ty,
                             'acquired': test.acquired,
                             'chips': test.chips,
                             'date': '0001-01-02'}).status == '200 OK'
    return True
    
    
def test_annual_prediction_runs_as_expected(client):
    '''
    As a blackmagic user, when I send tx, ty, acquired, month, day and chip list
    via HTTP POST, annual predictions are generated and saved to Cassandra
    so that they can be retrieved later.
    '''

    create_annual_prediction_test_data(client)    

    # test prediction

    
    response = client.post('/annual-prediction',
                           json={'tx': test.tx,
                                 'ty': test.ty,
                                 'chips': test.chips,
                                 'month': test.prediction_month,
                                 'day': test.prediction_day,
                                 'acquired': test.acquired})

    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
     
    assert response.status == '200 OK'
    assert get('tx', response.get_json()) == test.tx
    assert get('ty', response.get_json()) == test.ty
    assert get('acquired', response.get_json()) == test.acquired
    assert get('chips', response.get_json()) == test.chips
    assert get('month', response.get_json()) == test.prediction_month
    assert get('day', response.get_json()) == test.prediction_day
    assert get('exception', response.get_json(), None) == None

    assert len([p for p in predictions]) == 349194
    

def test_annual_prediction_bad_parameters(client):
    '''
    As a blackmagic user, when I don't send tx, ty, acquired, date and chips list
    via HTTP POST the HTTP status is 400 and the response body tells
    me the required parameters so that I can send a good request.
    '''

    # bad parameters
    tx = None
    ty = test.cy
    a = test.acquired
    month = test.prediction_month
    day = test.prediction_day
    chips = test.chips
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': a,
                                 'month': month,
                                 'day': day,
                                 'chips': chips})

    delete_annual_predictions(test.cx, test.cy)
    
    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
    
    assert response.status == '400 BAD REQUEST'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == a
    assert get('chips', response.get_json()) == chips
    assert get('month', response.get_json()) == month
    assert get('day', response.get_json()) == day
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0

    assert len(list(map(lambda x: x, predictions))) == 0

    
def test_annual_prediction_missing_model(client):
    '''
    As a blackmagic user, when I send tx, ty, acquired, month, day, and chips via HTTP POST
    and no trained xgboost model is found for the given tx/ty, an exception is raised
    with HTTP 500 so that prediction does not occur and the problem may be resolved.
    '''

    tx = test.missing_tx
    ty = test.missing_ty
    a = test.acquired
    month = test.prediction_month
    day = test.prediction_day
    chips = test.chips

    delete_annual_predictions(test.cx, test.cy)
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'chips': chips,
                                 'month': month,
                                 'day': day,
                                 'acquired': a})

    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == a
    assert get('month', response.get_json()) == month
    assert get('day', response.get_json()) == day
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, predictions))) == 0
    

def test_annual_prediction_delete_exception(client):
    '''
    As a blackmagic user, when an exception occurs deleting 
    predictions from Cassandra, an HTTP 500 is issued
    with a descriptive message so that the issue may be 
    investigated, corrected & retried.
    '''

    tx = test.tx
    ty = test.ty
    acquired = test.acquired
    month = test.prediction_month
    day = test.prediction_day
    chips = test.chips

    delete_annual_predictions(test.cx, test.cy)
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'month': month,
                                 'day': day,
                                 'chips': chips,
                                 'test_delete_exception': True})
    
    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('month', response.get_json()) == month
    assert get('day', response.get_json()) == day
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, predictions))) == 0

    
def test_annual_prediction_save_exception(client):
    '''
    As a blackmagic user, when an exception occurs saving 
    a predictions to Cassandra, an HTTP 500 is issued
    with a descriptive message so that the issue may be 
    investigated, corrected & retried.
    '''

    tx = test.tx
    ty = test.ty
    acquired = test.acquired
    month = test.prediction_month
    day = test.prediction_day
    chips = test.chips

    delete_annual_predictions(test.cx, test.cy)
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'month': month,
                                 'day': day,
                                 'chips': chips,
                                 'test_save_exception': True})
    
    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('month', response.get_json()) == month
    assert get('day', response.get_json()) == day
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, predictions))) == 0
