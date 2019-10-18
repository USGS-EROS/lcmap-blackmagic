#!/usr/bin/env python3

from blackmagic.blueprints import tile as t
from blackmagic.blueprints import segment as s
from blackmagic.blueprints import prediction as p

import click
import os
import requests

##############################################################################################################
# IAC Task API
##############################################################################################################
#
# HTTP GET
# train_work_url = 'http://lcmap-test.cr.usgs.gov/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0-training/tasks'
# content-type: application/json
# [{"id": "abc123", "param1": "value1", "param2": "value2"},
#  {"id": "def456", "param1": "value1", "param2": "value2"},
#  {"id": "ghi789", "param1": "value1", "param2": "value2"}]
#
# 
# HTTP POST - Start work
# train_work_url = 'http://lcmap-test.cr.usgs.gov/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0-training/tasks/<id>
# content-type: application/json
# {"code": 0}
#
#     HTTP 202 Accepted, HTTP 403 Forbidden, or HTTP 404 Not Found.
#     If forbidden or not found, go look for more work or quit.
#
# HTTP POST - Control check
# train_work_url = 'http://lcmap-test.cr.usgs.gov/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0-training/tasks/<id>'
# content-type: application/json
# {"code": 1}
#
#     HTTP 200 Ok and one of the following
#     {"command": "run"}
#     {"command": "stop"}
#
#     If command is run the task should continue normally.
#     If command is stop the task should halt as soon as possible and update the job url with {"code": 4}
#
# HTTP POST - Status update
# train_work_url = 'http://lcmap-test.cr.usgs.gov/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0-training/tasks/<id>'
# content-type: application/json
# {"code": 2, "message": "whatever is needed as long as its a string"}
#
# HTTP POST - Error
# train_work_url = 'http://lcmap-test.cr.usgs.gov/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0-training/tasks/<id>'
# content-type: application/json
# {"code": 3, "message": "whatever is needed as long as its a string"}
#
# HTTP POST - Stop
# train_work_url = 'http://lcmap-test.cr.usgs.gov/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0-training/tasks/<id>'
# content-type: application/json
# {"code": 4}
#
# HTTP POST - Finish
# train_work_url = 'http://lcmap-test.cr.usgs.gov/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0-training/tasks/<id>'
# content-type: application/json
# {"code": 5, "message": "whatever is needed as long as its a string"}
#
#############################################################################################################


cfg = {'train_work_url': None,
       'detect_work_url': None,
       'predict_work_url': None,

def tile_to_xy(tile, dataset):
    pass

def tile_to_chips(tile, dataset):
    pass

def snap(x, y, dataset):
    pass

def near(x, y, dataset):
    pass

@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))

@cli.command()  # @cli, not @click!
@click.argument('cx')
@click.argument('cy')
@click.argument('acquired')
def detect(cx, cy, acquired):
    click.echo('Detecting')

@cli.command()
@click.argument('tile')
@click.argument('date')
@click.argument('acquired')
def train(tile, date, acquired):

    # Go look for work
    # If work is found, negotiate to start working on it
    # If negotiation fails, go look for more work, else start work
    # Report status
    # Do more work
    # Report more status
    # Finish work
    # Report completion
    # Go away

    params = {'tx': 123,
              'ty': 456,
              'acquired': '1985/2002',
              'date': '2001-07-01'}

    click.echo('checking for work')
    click.echo('negotiating work start')
    click.echo('negotiation failed, checking for work')
    click.echo('negotiating work start')
    click.echo('starting work')
    click.echo(t.run(params))
    click.echo('work complete')
    click.echo('exiting')

@cli.command()
@click.argument('cx')
@click.argument('cy')
@click.argument('acquired')
@click.argument('month')
@click.argument('day')
def predict(cx, cy, acquired, month, day):
    click.echo('Predict:{}'.format(cx))

if __name__ == '__main__':
    cli()
