import json
import os
import pytest
import test

from blackmagic import app
from blackmagic import db
from cassandra.cluster import Cluster
from cytoolz import get
from cytoolz import reduce


def delete_annual_predictions(cx, cy):
    return db.execute_statements(app.cfg,
                                 [db.delete_annual_predictions(app.cfg, cx, cy)])


@pytest.fixture
def client():
    app.app.config['TESTING'] = True
    yield app.app.test_client()

    
def test_annual_prediction_runs_as_expected(client):
    '''
    As a blackmagic user, when I send tx, ty, acquired, month, day and chip list
    via HTTP POST, annual predictions are generated and saved to Cassandra
    so that they can be retrieved later.
    '''

    # prepopulate a chip of segments
    assert client.post('/segment',
                       json={'cx': test.cx,
                             'cy': test.cy,
                             'acquired': test.acquired}).status == '200 OK'
    
    # prepopulate a trained model
    assert client.post('/tile',
                       json={'tx': test.tx,
                             'ty': test.ty,
                             'acquired': test.acquired,
                             'chips': test.chips,
                             'date': test.training_date}).status == '200 OK'
    

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

    assert len([p for p in predictions]) == 10000
    

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
    

def test_annual_prediction_cassandra_exception(client):
    '''
    As a blackmagic user, when an exception occurs saving 
    a tile to Cassandra, an HTTP 500 is issued
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
                                 'test_cassandra_exception': True})
    
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
