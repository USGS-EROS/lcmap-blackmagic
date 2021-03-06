from blackmagic import app
from blackmagic.data import ceph
from cytoolz import do
from cytoolz import get
from cytoolz import reduce

import json
import os
import pytest
import test

_ceph = ceph.Ceph(app.cfg)
_ceph.start()

def delete_detections(cx, cy):
    return [_ceph.delete_chip(cx, cy),
            _ceph.delete_pixels(cx, cy),
            _ceph.delete_segments(cx, cy)]

@pytest.fixture
def client():
    app.app.config['TESTING'] = True
    yield app.app.test_client()

    
def test_segment_runs_as_expected(client):
    '''
    As a blackmagic user, when I send cx, cy, & acquired range
    via HTTP POST, change segments are detected and saved
    so that they can be retrieved later.
    '''
    
    response = client.post('/segment',
                           json={'cx': test.cx, 'cy': test.cy, 'acquired': test.acquired})

    chips = _ceph.select_chip(cx=test.cx, cy=test.cy)

    pixels = _ceph.select_pixels(cx=test.cx, cy=test.cy)
    print("PIXEL LENGTH:{}".format(len(pixels)))
    print("PIXEL TYPE:{}".format(type(pixels)))
    
    segments = _ceph.select_segments(cx=test.cx, cy=test.cy)
    
    assert response.status == '200 OK'
    assert get('cx', response.get_json()) == test.cx
    assert get('cy', response.get_json()) == test.cy
    assert get('acquired', response.get_json()) == test.acquired
    assert get('exception', response.get_json(), None) == None
    
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
    a = test.acquired

    delete_detections(test.cx, test.cy)
    
    response = client.post('/segment',
                           json={'cx': cx, 'cy': cy, 'acquired': a})

    chips = _ceph.select_chip(cx=test.cx, cy=test.cy)
    
    pixels = _ceph.select_pixels(cx=test.cx, cy=test.cy)
    
    segments = _ceph.select_segments(cx=test.cx, cy=test.cy)
    
    assert response.status == '400 BAD REQUEST'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0

    assert len(list(map(lambda x: x, chips))) == 0
    assert len(list(map(lambda x: x, pixels))) == 0
    assert len(list(map(lambda x: x, segments))) == 0


def test_segment_merlin_exception(client):
    '''
    As a blackmagic user, when an exception occurs creating a 
    timeseries from raster data, an HTTP 500 is issued with a 
    message describing the failure so that the issue may be resolved.
    '''

    cx = test.cx
    cy = test.cy
    a = 'not-a-date'

    delete_detections(test.cx, test.cy)
    
    response = client.post('/segment',
                           json={'cx': cx, 'cy': cy, 'acquired': a})

    chips = _ceph.select_chip(cx=test.cx, cy=test.cy)
    
    pixels = _ceph.select_pixels(cx=test.cx, cy=test.cy)
    
    segments = _ceph.select_segments(cx=test.cx, cy=test.cy)
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0

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

    chips = _ceph.select_chip(cx=test.cx, cy=test.cy)
    
    pixels = _ceph.select_pixels(cx=test.cx, cy=test.cy)
    
    segments = _ceph.select_segments(cx=test.cx, cy=test.cy)
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0

    assert len(list(map(lambda x: x, chips))) == 0
    assert len(list(map(lambda x: x, pixels))) == 0
    assert len(list(map(lambda x: x, segments))) == 0


def test_segment_detection_exception(client):
    '''
    As a blackmagic user, when an exception occurs running 
    change detection an HTTP 500 is issued with a message 
    describing the failure so that the issue may be 
    investigated, corrected & retried.
    '''

    cx = test.cx
    cy = test.cy
    a  = test.acquired

    delete_detections(test.cx, test.cy)
    
    response = client.post('/segment',
                           json={'cx': cx,
                                 'cy': cy,
                                 'acquired': a,
                                 'test_detection_exception': True})

    chips = _ceph.select_chip(cx=test.cx, cy=test.cy)
    
    pixels = _ceph.select_pixels(cx=test.cx, cy=test.cy)
    
    segments = _ceph.select_segments(cx=test.cx, cy=test.cy)
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0

    assert len(list(map(lambda x: x, chips))) == 0
    assert len(list(map(lambda x: x, pixels))) == 0
    assert len(list(map(lambda x: x, segments))) == 0


def test_segment_save_exception(client):
    '''
    As a blackmagic user, when an exception occurs saving 
    chips, pixels & segments, an HTTP 500 is issued
    with a descriptive message so that the issue may be 
    investigated, corrected & retried.
    '''

    cx = test.cx
    cy = test.cy
    a  = test.acquired

    delete_detections(test.cx, test.cy)
    
    response = client.post('/segment',
                           json={'cx': cx,
                                 'cy': cy,
                                 'acquired': a,
                                 'test_save_exception': True})

    chips = _ceph.select_chip(cx=test.cx, cy=test.cy)
    
    pixels = _ceph.select_pixels(cx=test.cx, cy=test.cy)
    
    segments = _ceph.select_segments(cx=test.cx, cy=test.cy)
    
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert get('cx', response.get_json()) == cx
    assert get('cy', response.get_json()) == cy
    assert get('acquired', response.get_json()) == a
    assert type(get('exception', response.get_json())) is str
    assert len(get('exception', response.get_json())) > 0

    assert len(list(map(lambda x: x, chips))) == 0
    assert len(list(map(lambda x: x, pixels))) == 0
    assert len(list(map(lambda x: x, segments))) == 0
