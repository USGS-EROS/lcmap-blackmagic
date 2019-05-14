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
    
    response = client.post('/annual-prediction',
                           json={'tx': test.tx,
                                 'ty': test.ty,
                                 'chips': test.chips,
                                 'month': test.prediction_month,
                                 'day': test.prediction_day,
                                 'acquired': test.a})

    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
     
    assert response.status == '200 OK'
    assert get('tx', response.get_json()) == test.tx
    assert get('ty', response.get_json()) == test.ty
    assert get('acquired', response.get_json()) == test.a
    assert get('chips', response.get_json()) == test.chips
    assert get('month', response.get_json()) == test.prediction_month
    assert get('day', response.get_json()) == test.prediction_day
    assert get('exception', response.get_json(), None) == None
    
    assert len(list(map(lambda x: x, predictions))) == 10000
    

def test_annual_prediction_bad_parameters(client):
    '''
    As a blackmagic user, when I don't send tx, ty, acquired, date and chips list
    via HTTP POST the HTTP status is 400 and the response body tells
    me the required parameters so that I can send a good request.
    '''

    # bad parameters
    tx = None
    ty = test.cy
    a = test.a
    date = test.training_date
    chips = test.chips
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': a,
                                 'date': date,
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
    assert get('date', response.get_json()) == date
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0

    assert len(list(map(lambda x: x, predictions))) == 0


def test_annual_prediction_merlin_exception(client):
    '''
    As a blackmagic user, when an exception occurs creating a 
    timeseries from aux data, an HTTP 500 is issued with a 
    message describing the failure so that the issue may be resolved.
    '''

    tx = test.tx
    ty = test.ty
    a = 'not-a-date'
    date = test.training_date
    chips = test.chips

    delete_annual_predictions(test.cx, test.cy)
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'chips': chips,
                                 'date': date,
                                 'acquired': a})

    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == a
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, predictions))) == 0
    

def test_annual_prediction_training_exception(client):
    '''
    As a blackmagic user, when an exception occurs 
    training an xgboost model an HTTP 500 is issued with a message 
    describing the failure so that the issue may be 
    investigated, corrected & retried.
    '''

    tx = test.tx
    ty = test.ty
    a = test.a
    date = test.training_date
    chips = test.chips

    delete_annual_predictions(test.cx, test.cy)
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': a,
                                 'date': date,
                                 'chips': chips,
                                 'test_training_exception': True})
    
    predictions = db.execute_statement(cfg=app.cfg,
                                       stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                         cx=test.cx,
                                                                         cy=test.cy))
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == a
    assert get('date', response.get_json()) == date
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
    a = test.a
    date = test.training_date
    chips = test.chips

    delete_annual_predictions(test.cx, test.cy)
    
    response = client.post('/annual-prediction',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': a,
                                 'date': date,
                                 'chips': chips,
                                 'test_cassandra_exception': True})
    
    annual_predictions = db.execute_statement(cfg=app.cfg,
                                              stmt=db.select_annual_predictions(cfg=app.cfg,
                                                                                cx=test.cx,
                                                                                cy=test.cy))
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == a
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, predictions))) == 0
