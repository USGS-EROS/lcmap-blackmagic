import json
import os
import pytest
import requests
import test

from blackmagic import app
from blackmagic import db
from blackmagic.blueprints import tile
from cassandra.cluster import Cluster
from cytoolz import first
from cytoolz import get
from cytoolz import reduce


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
    acquired = test.a
    chips    = test.chips
    date     = test.date
        
    # train a model based on those segments and
    # the aux data


        # prepopulate a chip of segments
    assert client.post('/segment',
                       json={'cx': test.cx,
                             'cy': test.cy,
                             'acquired': test.a}).status == '200 OK'
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date})

    assert response.status == '200 OK'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == chips
    assert get('exception', response.get_json(), None) == None


def test_tile_missing_segments(client):
    '''
    As a blackmagic user, when there are no segments available
    to match up with aux data, HTTP 500 is issued with a message
    indicating no segments were found so that the issue may be 
    resolved (by repairing the database, running change detection,
    etc)
    '''
    
    pass


def test_tile_missing_aux(client):
    '''
    As a blackmagic user, when there is no aux data available
    to match up with segments, HTTP 500 is issued with a message
    indicating missing aux data so that the issue may be 
    corrected.
    '''
    
    pass


def test_tile_bad_parameters(client):
    '''
    As a blackmagic user, when I don't send tx, ty, acquired, date & chips
    via HTTP POST the HTTP status is 400 and the response body tells
    me the required parameters so that I can send a good request.
    '''

    tx       = "not-an-integer"
    ty       = test.ty
    acquired = test.a
    chips    = test.chips
    date     = test.date
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date})

    assert response.status == '400 BAD REQUEST'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0


def test_tile_data_exception(client):
    '''
    As a blackmagic user, when an exception occurs retrieving 
    and constructing training data, an HTTP 500 is issued with a 
    message describing the failure so that the issue may be resolved.
    '''

    tx       = test.tx
    ty       = test.ty
    acquired = test.a
    chips    = test.chips
    date     = test.date
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date,
                                 'test_data_exception': True})

    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0


def test_tile_training_exception(client):
    '''
    As a blackmagic user, when an exception occurs training 
    a model, an HTTP 500 is issued with a message describing 
    the failure so that the issue may be investigated & resolved.
    '''

    tx       = test.tx
    ty       = test.ty
    acquired = test.a
    chips    = test.chips
    date     = test.date
     
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date,
                                 'test_training_exception': True})

    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0


def test_tile_cassandra_exception(client):
    '''
    As a blackmagic user, when an exception occurs saving 
    models to Cassandra, an HTTP 500 is issued
    with a descriptive message so that the issue may be 
    investigated, corrected & retried.
    '''

    tx       = test.tx
    ty       = test.ty
    acquired = test.a
    chips    = test.chips
    date     = test.date
    
    response = client.post('/tile',
                           json={'tx': tx,
                                 'ty': ty,
                                 'acquired': acquired,
                                 'chips': chips,
                                 'date': date,
                                 'test_cassandra_exception': True})

    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('tx', response.get_json()) == tx
    assert get('ty', response.get_json()) == ty
    assert get('acquired', response.get_json()) == acquired
    assert get('date', response.get_json()) == date
    assert get('chips', response.get_json()) == chips
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0


def test_tile_aux():
    pass


def test_tile_segments():
    pass


def test_tile_datefilter():
    pass


def test_tile_combine():
    pass


def test_tile_format():
    pass


def test_tile_independent():
    pass


def test_tile_dependent():
    pass


def test_tile_watchlist():
    pass


def test_tile_pipeline():
    pass


def test_tile_parameters():
    pass


def test_tile_log_request():
    pass


def test_tile_exception_handler():
    pass


def test_tile_data():
    pass


def test_tile_counts():
    data1 = [0, 1, 2]
    data2 = [1, 2, 3]
    data3 = [2, 3, 4]

    assert get(0, tile.counts(data1)) == 1
    assert get(1, tile.counts(data2)) == 1
    assert get(2, tile.counts(data3)) == 1


def test_tile_statistics():
    ctx = {'data': [[0, 1, 2],
                    [0, 2, 3],
                    [1, 1, 2],
                    [2, 3, 4],
                    [2, 4, 5],
                    [2, 6, 7]]}

    stats = get('statistics', tile.statistics(ctx))

    assert get(0, stats) == 2
    assert get(1, stats) == 1
    assert get(2, stats) == 3


def test_tile_randomize():
    pass


def test_sample_sizes():
    pass


def test_tile_sample():
    
    ctx = {'sample_sizes': {0: 1, 1: 4, 2: 3},
           'data': [[0, 1, 2],
                    [0, 2, 3],
                    [0, 3, 4],
                    [1, 1, 2],
                    [1, 2, 3],
                    [1, 3, 4],
                    [1, 4, 5],
                    [1, 5, 6],
                    [2, 0, 0],
                    [2, 0, 1],
                    [2, 0, 2],
                    [2, 0, 3]]}
    
    sampled = get('data', tile.sample(ctx))
   
    assert len(list(filter(lambda x: first(x) == 0, sampled))) == 1
    assert len(list(filter(lambda x: first(x) == 1, sampled))) == 4
    assert len(list(filter(lambda x: first(x) == 2, sampled))) == 3
    

def test_tile_train():
    pass


def test_tile_save():
    pass


def test_tile_respond():
    pass
