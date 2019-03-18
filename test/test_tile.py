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
def test_tile_runs_as_expected(client):
    '''
    As a blackmagic user, when I send tx, ty, date & chips
    via HTTP POST, an xgboost model is trained and saved
    to Cassandra so that change segments may be classified.
    '''
    pass


def test_tile_bad_parameters(client):
    '''
    As a blackmagic user, when I don't send tx, ty, date & chips
    via HTTP POST the HTTP status is 400 and the response body tells
    me the required parameters so that I can send a good request.
    '''
    pass


def test_tile_data_exception(client):
    '''
    As a blackmagic user, when an exception occurs retrieving 
    and building training data, an HTTP 500 is issued with a 
    message describing the failure so that the issue may be resolved.
    '''
    pass


def test_tile_training_exception(client):
    '''
    As a blackmagic user, when an exception occurs training 
    a model, an HTTP 500 is issued with a  message describing 
    the failure so that the issue may be investigated & resolved.
    '''
    pass


def test_tile_cassandra_exception(client):
    '''
    As a blackmagic user, when an exception occurs saving 
    models to Cassandra, an HTTP 500 is issued
    with a descriptive message so that the issue may be 
    investigated, corrected & retried.
    '''
    pass
