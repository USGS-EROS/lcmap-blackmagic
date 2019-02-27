import json
import os
import pytest
import test

from blackmagic import app
from blackmagic import db
from cassandra.cluster import Cluster
from cytoolz import get
from cytoolz import reduce


def delete_detections(cx, cy):
    return db.execute_statements(app.cfg,
                                 [db.delete_chip(app.cfg, cx, cy),
                                  db.delete_pixel(app.cfg, cx, cy),
                                  db.delete_segment(app.cfg, cx, cy)])

@pytest.fixture
def client():
    app.app.config['TESTING'] = True
    yield app.app.test_client()

    
@test.vcr.use_cassette(test.cassette)    
def test_segment_runs_as_expected(client):
    '''
    As a blackmagic user, when I send cx, cy, & acquired range
    via HTTP POST, change segments are detected and saved to Cassandra
    so that they can be retrieved later.
    '''
    
    response = client.post('/segment',
                           json={'cx': test.cx, 'cy': test.cy, 'acquired': test.a})

    chips = db.execute_statement(cfg=app.cfg,
                                 stmt=db.select_chip(cfg=app.cfg,
                                                     cx=test.cx,
                                                     cy=test.cy))
    
    pixels = db.execute_statement(cfg=app.cfg,
                                  stmt=db.select_pixel(cfg=app.cfg,
                                                       cx=test.cx,
                                                       cy=test.cy))
    
    segments = db.execute_statement(cfg=app.cfg,
                                    stmt=db.select_segment(cfg=app.cfg,
                                                           cx=test.cx,
                                                           cy=test.cy))
    assert response.status == '200 OK'
    assert get('cx', response.get_json()) == test.cx
    assert get('cy', response.get_json()) == test.cy
    assert get('acquired', response.get_json()) == test.a

    assert len(list(map(lambda x: x, chips))) == 1
    assert len(list(map(lambda x: x, pixels))) == 10000
    assert len(list(map(lambda x: x, segments))) == 10000

    
def test_segment_bad_parameters(client):
    '''
    As a blackmagic user, when I don't send cx, cy, & acquired range
    via HTTP POST the HTTP status is 400 and the response body tells
    me the required parameters so that I can send a good request.
    '''

    # bad parameters
    cx = None
    cy = test.cy
    a = test.a

    delete_detections(test.cx, test.cy)
    
    response = client.post('/segment',
                           json={'cx': cx, 'cy': cy, 'acquired': a})

    chips = db.execute_statement(cfg=app.cfg,
                                 stmt=db.select_chip(cfg=app.cfg,
                                                     cx=test.cx,
                                                     cy=test.cy))
    
    pixels = db.execute_statement(cfg=app.cfg,
                                  stmt=db.select_pixel(cfg=app.cfg,
                                                       cx=test.cx,
                                                       cy=test.cy))
    
    segments = db.execute_statement(cfg=app.cfg,
                                    stmt=db.select_segment(cfg=app.cfg,
                                                           cx=test.cx,
                                                           cy=test.cy))
    assert response.status == '400 BAD REQUEST'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a

    assert len(list(map(lambda x: x, chips))) == 0
    assert len(list(map(lambda x: x, pixels))) == 0
    assert len(list(map(lambda x: x, segments))) == 0

    
def test_segment_merlin_exception(client):
    '''
    As a blackmagic user, when an exception occurs creating a 
    timeseries from raster data, an HTTP 500 is issued with a 
    message describing the failure so that the issue may be resolved.
    '''

    cx = 'not-an-integer'
    cy = test.cy
    a = test.a

    delete_detections(test.cx, test.cy)
    
    response = client.post('/segment',
                           json={'cx': cx, 'cy': cy, 'acquired': a})

    chips = db.execute_statement(cfg=app.cfg,
                                 stmt=db.select_chip(cfg=app.cfg,
                                                     cx=test.cx,
                                                     cy=test.cy))
    
    pixels = db.execute_statement(cfg=app.cfg,
                                  stmt=db.select_pixel(cfg=app.cfg,
                                                       cx=test.cx,
                                                       cy=test.cy))
    
    segments = db.execute_statement(cfg=app.cfg,
                                    stmt=db.select_segment(cfg=app.cfg,
                                                           cx=test.cx,
                                                           cy=test.cy))
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a

    assert len(list(map(lambda x: x, chips))) == 0
    assert len(list(map(lambda x: x, pixels))) == 0
    assert len(list(map(lambda x: x, segments))) == 0
    

def test_segment_merlin_no_input_data(client):
    '''
    As a blackmagic user, when no input data is available
    to build a timeseries, an HTTP 500 is issued with a message
    indicating "no input data" so that I know change detection
    cannot run for this time & space.
    '''

    cx = test.cx
    cy = test.cy
    a = '1975/1976'

    delete_detections(test.cx, test.cy)
    
    response = client.post('/segment',
                           json={'cx': cx, 'cy': cy, 'acquired': a})

    chips = db.execute_statement(cfg=app.cfg,
                                 stmt=db.select_chip(cfg=app.cfg,
                                                     cx=test.cx,
                                                     cy=test.cy))
    
    pixels = db.execute_statement(cfg=app.cfg,
                                  stmt=db.select_pixel(cfg=app.cfg,
                                                       cx=test.cx,
                                                       cy=test.cy))
    
    segments = db.execute_statement(cfg=app.cfg,
                                    stmt=db.select_segment(cfg=app.cfg,
                                                           cx=test.cx,
                                                           cy=test.cy))
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a

    assert len(list(map(lambda x: x, chips))) == 0
    assert len(list(map(lambda x: x, pixels))) == 0
    assert len(list(map(lambda x: x, segments))) == 0

    
def test_segment_detection_exception():
    '''
    As a blackmagic user, when an exception occurs running 
    change detection an HTTP 500 is issued with a message 
    describing the failure so that the issue may be 
    investigated, corrected & retried.
    '''
    
    # make sure return is 500 with expected body
    # make sure log messages are as expected

    # trigger exception by pointing to wrong Chipmunk and pulling aux
    
    pass


def test_segment_cassandra_exception():
    '''
    As a blackmagic user, when an exception occurs saving 
    chips, pixels & segments to Cassandra, an HTTP 500 is issued
    with a descriptive message so that the issue may be 
    investigated, corrected & retried.
    '''
    
    # make sure return is 500 with expected body
    # make sure log messages are as expected

    # trigger exception by deleting keyspace
    
    pass


