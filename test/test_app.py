import json
import os
import pytest
import test

from blackmagic import app
from blackmagic import db
from cassandra.cluster import Cluster
from cytoolz import get
from cytoolz import reduce


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

    cx = -2061585
    cy = 1922805
    a  = '1980/2019'
    
    response = client.post('/segment',
                           json={'cx': cx, 'cy': cy, 'acquired': a})

    chips = db.execute_statement(cfg=app.cfg,
                                 stmt=db.select_chip(cfg=app.cfg,
                                                     cx=cx,
                                                     cy=cy))
    
    pixels = db.execute_statement(cfg=app.cfg,
                                  stmt=db.select_pixel(cfg=app.cfg,
                                                       cx=cx,
                                                       cy=cy))
    
    segments = db.execute_statement(cfg=app.cfg,
                                    stmt=db.select_segment(cfg=app.cfg,
                                                           cx=cx,
                                                           cy=cy))
    assert response.status == '200 OK'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a

    assert len(list(map(lambda x: x, chips))) == 1
    assert len(list(map(lambda x: x, pixels))) == 10000
    assert len(list(map(lambda x: x, segments))) == 10000

    
def test_segment_bad_parameters():
    '''
    As a blackmagic user, when I don't send cx, cy, & acquired range
    via HTTP POST the HTTP status is 400 and the response body tells
    me the required parameters so that I can send a good request.
    '''
    
    # make sure return is 400 with expected body
    # make sure detection did not run or save anything
    # make sure log messages are as expected

    # trigger exception by not passing parameters
    pass


def test_segment_merlin_exception():
    '''
    As a blackmagic user, when an exception occurs creating a 
    timeseries from raster data, an HTTP 500 is issued with a 
    message describing the failure so that the issue may be resolved.
    '''
    
    # make sure return is 500 with expected body
    # make sure detection did not run or save anything
    # make sure log messages are as expected

    # trigger exception by passing Merlin bad parameters
    
    pass


def test_segment_merlin_no_input_data():
    '''
    As a blackmagic user, when no input data is available
    to build a timeseries, an HTTP 500 is issued with a message
    indicating "no input data" so that I know change detection
    cannot run for this time & space.
    '''
    
    # make sure return is 500 with expected body
    #    -- should this really be an error though?
    # make sure detection did not run or save anything
    # make sure log messages are as expected

    # trigger exception by asking for area with no data
    pass


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


