import os
import vcr as _vcr

# set environment so blackmagic will point to local cassandra & chipmunk
# Chipmunk does not get started and run during CICD testing.  We use
# vcrpy to prerecord and replay HTTP responses

os.environ['CASSANDRA_HOST']     = 'localhost'
os.environ['CASSANDRA_PORT']     = '9042'
os.environ['CASSANDRA_USER']     = 'cassandra'
os.environ['CASSANDRA_PASS']     = 'cassandra'
os.environ['CASSANDRA_KEYSPACE'] = 'blackmagic_test'
os.environ['ARD_URL']            = 'http://localhost:5656'
os.environ['AUX_URL']            = 'http://localhost:5656'
os.environ['CPUS_PER_WORKER']    = '1'
os.environ['WORKERS']            = '1'
os.environ['WORKER_TIMEOUT']     = '12000'

segment_cassette = 'test/resources/segment-cassette.yaml'
tile_cassette = 'test/resources/tile-cassette.yaml'
#vcr = _vcr.VCR(record_mode='new_episodes')
vcr = _vcr.VCR(record_mode='once')

tx = -2115585
ty = 1964805
cx = -2061585
cy = 1922805
chips = [[cx, cy],]
date = '2001-07-01'
a  = '1980/2019'
