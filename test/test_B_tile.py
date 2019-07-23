from blackmagic import app
from blackmagic.blueprints import tile
from blackmagic.data import ceph
from collections import namedtuple
from cytoolz import count
from cytoolz import do
from cytoolz import first
from cytoolz import get
from cytoolz import reduce

import json
import os
import pytest
import numpy
import requests
import test

_ceph = ceph.Ceph(app.cfg)
_ceph.start()

def delete_tile(tx, ty):
    return _ceph.delete_tile(tx, ty)


@pytest.fixture
def client():
    app.app.config['TESTING'] = True
    yield app.app.test_client()


def test_tile_runs_as_expected(client):
    '''
    As a blackmagic user, when I send tx, ty, date & chips
    via HTTP POST, an xgboost model is trained and saved
    to Cassandra so that change segments may be classified.
    '''

    tx       = test.tx
    ty       = test.ty
    cx       = test.cx
    cy       = test.cy
    acquired = test.acquired
    chips    = test.chips
    date     = test.training_date
        
    # train a model based on those segments and
    # the aux data


    # prepopulate a chip of segments
    assert client.post('/segment',
                       json={'cx': test.cx,
                             'cy': test.cy,
                             'acquired': test.acquired}).status == '200 OK'
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date})

    tiles = _ceph.select_tile(tx=test.tx, ty=test.ty)
    
    assert response.status == '200 OK'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == count(chips)
    assert get('exception', response.get_json(), None) == None
    assert len(list(map(lambda x: x, tiles))) == 1


def test_tile_bad_parameters(client):
    '''
    As a blackmagic user, when I don't send tx, ty, acquired, date & chips
    via HTTP POST the HTTP status is 400 and the response body tells
    me the required parameters so that I can send a good request.
    '''

    tx       = "not-an-integer"
    ty       = test.ty
    acquired = test.acquired
    chips    = test.chips
    date     = test.training_date

    delete_tile(test.tx, test.ty)
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date})

    tiles = _ceph.select_tile(tx=test.tx, ty=test.ty)

    assert response.status == '400 BAD REQUEST'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == count(chips)
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, tiles))) == 0
    

def test_tile_data_exception(client):
    '''
    As a blackmagic user, when an exception occurs retrieving 
    and constructing training data, an HTTP 500 is issued with a 
    message describing the failure so that the issue may be resolved.
    '''

    tx       = test.tx
    ty       = test.ty
    acquired = test.acquired
    chips    = test.chips
    date     = test.training_date

    delete_tile(test.tx, test.ty)

    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date,
                                 'test_data_exception': True})

    tiles = _ceph.select_tile(tx=test.tx, ty=test.ty)

    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == count(chips)
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, tiles))) == 0


def test_tile_training_exception(client):
    '''
    As a blackmagic user, when an exception occurs training 
    a model, an HTTP 500 is issued with a message describing 
    the failure so that the issue may be investigated & resolved.
    '''

    tx       = test.tx
    ty       = test.ty
    acquired = test.acquired
    chips    = test.chips
    date     = test.training_date

    delete_tile(test.tx, test.ty)
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date,
                                 'test_training_exception': True})

    tiles = _ceph.select_tile(tx=test.tx, ty=test.ty)
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == count(chips)
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, tiles))) == 0


def test_tile_cassandra_exception(client):
    '''
    As a blackmagic user, when an exception occurs saving 
    models to Cassandra, an HTTP 500 is issued
    with a descriptive message so that the issue may be 
    investigated, corrected & retried.
    '''

    tx       = test.tx
    ty       = test.ty
    acquired = test.acquired
    chips    = test.chips
    date     = test.training_date

    delete_tile(test.tx, test.ty)
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date,
                                 'test_cassandra_exception': True})

    tiles = _ceph.select_tile(tx=test.tx, ty=test.ty)
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == count(chips)
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0
    assert len(list(map(lambda x: x, tiles))) == 0

    
def test_segments_filter():
    
    inputs = {'date': '1980-01-01',
              'segments': [{'sday': '1970-01-01', 'eday': '1990-01-01'},
                           {'sday': '1963-01-01', 'eday': '1964-01-01'}]}

    expected = {'date': '1980-01-01',
                'segments': [{'sday': '1970-01-01', 'eday': '1990-01-01'}]}
    
    outputs = tile.segments_filter(inputs)

    assert expected == outputs

    
def test_tile_statistics():
    ctx = {'data': numpy.array([[0, 1, 2],
                                [0, 2, 3],
                                [1, 1, 2],
                                [1, 1, 1],
                                [1, 1, 1],
                                [2, 2, 2],
                                [2, 2, 2],
                                [2, 3, 4],
                                [2, 4, 5],
                                [2, 6, 7]])}

    stats = get('statistics', tile.statistics(ctx))

    assert numpy.array_equal(stats[0], numpy.array([0, 1, 2]))
    assert numpy.array_equal(stats[1], numpy.array([0.20, 0.30, 0.50]))
           

def test_tile_randomize():
    pass


def test_tile_sample():
    
    ctx = {'independent': numpy.array([[0, 1],
                                       [4, 4],
                                       [2, 3],
                                       [4, 5],
                                       [6, 7],
                                       [8, 9],
                                       [9, 10]]),
           'dependent': numpy.array([0, 1, 0, 2, 0, 2, 0]),
           'statistics': (numpy.array([0,1,2]),
                          numpy.array([0.5714, 0.1428, 0.2857]))}

    cfg = {'xgboost': {'target_samples': 50, 'class_min': 2, 'class_max': 3}}
           
    s = tile.sample(ctx, cfg)
    i = s['independent']
    d = s['dependent']

    assert numpy.array_equal(d, [0, 0, 0, 1, 2, 2])    

    
def test_tile_train():
    pass


def test_tile_save():
    pass


def test_tile_respond():
    pass
