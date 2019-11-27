import os

# set environment so blackmagic will point to local s3 & chipmunk
# Chipmunk does not get started and run during CICD testing.  We use
# vcrpy to prerecord and replay HTTP responses

os.environ['S3_URL'] = 'http://localhost:4572'
os.environ['S3_ACCESS_KEY'] = ''
os.environ['S3_SECRET_KEY'] = ''
os.environ['S3_BUCKET'] = 'blackmagic-test-bucket'
os.environ['ARD_URL']            = 'http://localhost:9999'
os.environ['AUX_URL']            = 'http://localhost:9999'
os.environ['CPUS_PER_WORKER']    = '1'
os.environ['WORKERS']            = '1'
os.environ['WORKER_TIMEOUT']     = '12000'

tx = -2115585
ty = 1964805
cx = -2061585
cy = 1922805
chips = [[cx, cy],]
training_date = '0001-01-01'
acquired = '1980/2019'
prediction_month='07'
prediction_day='01'

missing_tx = 0
missing_ty = 0
