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

import os
import requests

cfg = {'train': os.environ['training_task_url'],
       'detect': os.environ['detect_task_url'],
       'predict': os.environ['prediction_task_url']}

def task_url(service, taskid):
    url = cfg['service']
    return '/'.join([url, taskid])

def tasks(service):
    resp = requests.get(cfg['service'])

    if r.ok:
        return r.json()
    else:
        return None

def start(service, taskid):
    body = {'code': 0}
    resp = requests.POST(url=task_url(service, taskid), data=body)
    return resp.status_code

def control(service, taskid):
    body = {'code': 1}
    resp = requests.POST(url=task_url(service, taskid), data=body)
    
    if resp.ok:
        return resp.json().get("command")
    else:
        # can dump some debug log messages here 
        return None

def status(service, taskid, message):
    body = {'code': 2, 'message': message}
    resp = requests.POST(url=task_url(service, taskid), data=body)
    return resp.status_code

def error(service, taskid, message):
    body = {'code': 3, 'message': message}
    resp = requests.POST(url=task_url(service, taskid), data=body)
    return resp.status_code

def stop(service, taskid):
    body = {'code': 4}
    resp = requests.POST(url=task_url(service, taskid), data=body)
    return resp.status_code

def finish(service, taskid):
    body = {'code': 5}
    resp = requests.POST(url=task_url(service, taskid), data=body)
    return resp.status_code
