from blackmagic.data import Storage
from cytoolz import first
from cytoolz import get
from interface import implements

import blackmagic
import boto3
import json
import gzip
import logging
import os

"""Blackmagic ceph provides the capability of storing and retrieving
all Blackmagic data with Ceph (S3).

The target url scheme is:

http://host:port/keyspace/hhh/vvv/category/items.json|.tif|.xxx

Examples:

http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/tile/xgboost.bin
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/chip/123--456.json
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/pixel/123--456.json
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/segment/123--456.json
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/prediction/123--456.json
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/json/change/123--456.json
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/json/cover/123--456.json
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/raster/change/LCMAP-CU-024007-19870701-V01-SCTIME.tif
http://host:port/ard-cu-c01-v01-aux-cu-v01-ccdc-1-0/001/002/raster/cover/LCMAP-CU-024007-19870701-V01-SCTIME.tif

All parts of the path between the keyspace (bucket) and the item name (key) are derived from the Ceph prefix.
This works as directory listings via HTTP, thus an easy way to inventory the contents is to walk the directory
structure from the keyspace down.  At the item (key) level, the number of items that should be present is 
known ahead of time while varying based on the item type.

For /tile, there should be 1 and only 1 item, a trained xgboost model.
For /chip, /pixel, /segment, /prediction, /json/change & /json/cover there should be 2500 partitions in each.
For /raster/change & /raster/cover, there should be 5 tifs each.

Note that /json/change, /json/cover, /raster/change & /raster/cover are produced by lcmap-gaia, not lcmap-blackmagic.
It is referenced here because they share the keyspace/bucket and should be stored consistently.

Within Ceph, each item is stored with the proper content-type set, thus when accessed over HTTP the requesting
client should be able to determine the content type and handle it appropriately.  Content-length should 
also be set.

"""

logger = logging.getLogger(__name__)

cfg = {'s3_url': os.environ.get('S3_URL', 'http://localhost:4572'),
       's3_access_key': os.environ.get('S3_ACCESS_KEY', ''),
       's3_secret_key': os.environ.get('S3_SECRET_KEY', ''),
       's3_bucket': os.environ.get('S3_BUCKET', 'blackmagic-test-bucket')}

   
class Ceph(implements(Storage)):

    def __init__(self, cfg):
        self.url = cfg['s3_url']
        self.bucket_name = cfg['s3_bucket']
        self.access_key = cfg['s3_access_key']
        self.secret_key = cfg['s3_secret_key']
        self.cfg = cfg
        
    def setup(self):   
        s3 = boto3.resource('s3',
                            endpoint_url=self.url,
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key)

        """{'ResponseMetadata': {'HTTPStatusCode': 200, 
                                 'HTTPHeaders': {'server': 'BaseHTTP/0.6 Python/3.6.8', 
                                 'date': 'Tue, 16 Jul 2019 16:11:07 GMT', 
                                 'content-type': 'application/xml; charset=utf-8', 
                                 'content-length': '159', 
                                 'last-modified': 'Tue, 16 Jul 2019 16:11:07 GMT', 
                                 'access-control-allow-origin': '*', 
                                 'access-control-allow-methods': 'HEAD,GET,PUT,POST,DELETE,OPTIONS,PATCH', 
                                 'access-control-allow-headers': 'authorization,content-type,content-md5,cache-control,x-amz-content-sha256,x-amz-date,x-amz-security-token,x-amz-user-agent,x-amz-acl,x-amz-version-id', 
                                 'access-control-expose-headers': 'x-amz-version-id'}, 
            'RetryAttempts': 0}}"""

        
        return s3.Bucket(self.bucket_name).create()

    def start(self):
        
        self.client = boto3.client('s3',
                                   endpoint_url=self.url,
                                   aws_access_key_id=self.access_key,
                                   aws_secret_access_key=self.secret_key)

        resource = boto3.resource('s3',
                                   endpoint_url=self.url,
                                   aws_access_key_id=self.access_key,
                                   aws_secret_access_key=self.secret_key)
        
        self.bucket = resource.Bucket(self.bucket_name)

    def stop(self):
        self.bucket = None

    def select_tile(self, tx, ty):
        try:
            return self._get_json(self._tile_key(tx=tx, ty=ty))
        except self.client.exceptions.NoSuchKey:
            return []
    
    def select_chip(self, cx, cy):
        try:
            return self._get_json(self._chip_key(cx=cx, cy=cy))
        except self.client.exceptions.NoSuchKey:
            return []

    def select_pixels(self, cx, cy):
        try:
            return self._get_json(self._pixel_key(cx=cx, cy=cy))
        except self.client.exceptions.NoSuchKey:
            return []

    def select_segments(self, cx, cy):
        try:
            return self._get_json(self._segment_key(cx=cx, cy=cy))
        except self.client.exceptions.NoSuchKey:
            return []

    def select_predictions(self, cx, cy):
        try:
            return self._get_json(self._prediction_key(cx=cx, cy=cy))
        except self.client.exceptions.NoSuchKey:
            return []

    def insert_tile(self, tx, ty, model):

        def tile(tx, ty, tile):
            return {'tx': tx,
                    'ty': ty,
                    'model': model}

        t = tile(tx, ty, model)
        
        return self._put_json(self._tile_key(tx, ty),
                              [t],
                              compress=True)
    
    def insert_chip(self, detections):

        def chip(detection):
            return {'cx':    detection['cx'],
                    'cy':    detection['cy'],
                    'dates': detection['dates']}

        c = chip(first(detections))

        return self._put_json(self._chip_key(c['cx'], c['cy']),
                              [c],
                              compress=True)

    def insert_pixels(self, detections):
        
        def pixel(detection):
            return {'cx':   detection['cx'],
                    'cy':   detection['cy'],
                    'px':   detection['px'],
                    'py':   detection['py'],
                    'mask': detection['mask']}

        pixels = [pixel(d) for d in detections]
        
        return self._put_json(self._pixel_key(first(pixels)['cx'], first(pixels)['cy']),
                              pixels,
                              compress=True)

    def insert_segments(self, detections):

        def segment(detection):
            return {'cx':     detection['cx'],
                    'cy':     detection['cy'],
                    'px':     detection['px'],
                    'py':     detection['py'],
                    'sday':   detection['sday'],
                    'eday':   detection['eday'],
                    'bday':   detection['bday'],
                    'chprob': detection['chprob'],
                    'curqa':  detection['curqa'],
                    'blcoef': detection['blcoef'],
                    'blint':  detection['blint'],
                    'blmag':  detection['blmag'],
                    'blrmse': detection['blrmse'],
                    'grcoef': detection['grcoef'],
                    'grint':  detection['grint'],
                    'grmag':  detection['grmag'],
                    'grrmse': detection['grrmse'],
                    'nicoef': detection['nicoef'],
                    'niint':  detection['niint'],
                    'nimag':  detection['nimag'],
                    'nirmse': detection['nirmse'],
                    'recoef': detection['recoef'],
                    'reint':  detection['reint'],
                    'remag':  detection['remag'],
                    'rermse': detection['rermse'],
                    's1coef': detection['s1coef'],
                    's1int':  detection['s1int'],
                    's1mag':  detection['s1mag'],
                    's1rmse': detection['s1rmse'],
                    's2coef': detection['s2coef'],
                    's2int':  detection['s2int'],
                    's2mag':  detection['s2mag'],
                    's2rmse': detection['s2rmse'],
                    'thcoef': detection['thcoef'],
                    'thint':  detection['thint'],
                    'thmag':  detection['thmag'],
                    'thrmse': detection['thrmse']}

        segments = [segment(d) for d in detections]
        
        return self._put_json(self._segment_key(first(detections)['cx'], first(detections)['cy']),
                              segments,
                              compress=True)

    def insert_predictions(self, predictions):

        def prediction(p):
            return {'cx':   p['cx'],
                    'cy':   p['cy'],
                    'px':   p['px'],
                    'py':   p['py'],
                    'sday': p['sday'],
                    'eday': p['eday'],
                    'pday': p['pday'],
                    'prob': p['prob']}

        preds = [prediction(p) for p in predictions]

        if len(preds) > 0:
            return self._put_json(self._prediction_key(first(predictions)['cx'],
                                                       first(predictions)['cy']),
                                  preds,
                                  compress=True)
        else:
            msg = "No predictions supplied to ceph.insert_predictions... skipping save"
            logger.warn(msg)
            return msg
            

    def delete_tile(self, tx, ty):
        return self._delete(self._tile_key(tx=tx, ty=ty))
    
    def delete_chip(self, cx, cy):
        return self._delete(self._chip_key(cx=cx, cy=cy))

    def delete_pixels(self, cx, cy):
        return self._delete(self._pixel_key(cx=cx, cy=cy))

    def delete_segments(self, cx, cy):
        return self._delete(self._segment_key(cx=cx, cy=cy))

    def delete_predictions(self, cx, cy):
        return self._delete(self._prediction_key(cx=cx, cy=cy))

    def _get_bin(self, key):
        o = self.client.get_object(Bucket=self.bucket_name, Key=key)

        if get('ContentEncoding', o, None) == 'gzip':
            v = gzip.decompress(o['Body'].read())
        else:
            v = o['Body'].read()

        return v

    def _put_bin(self, key, value, compress=True):

        v = value

        if compress:
            v = gzip.compress(v)
            
            return self.bucket.put_object(Bucket=self.bucket_name,
                                          Key=key,
                                          Body=v,
                                          ACL='public-read',
                                          ContentType='application/octet-stream',
                                          ContentLength=len(v),
                                          ContentEncoding='gzip')
        else:
            return self.bucket.put_object(Bucket=self.bucket_name,
                                          Key=key,
                                          Body=v,
                                          ACL='public-read',
                                          ContentType='application/octet-stream',
                                          ContentLength=len(v))
    
    def _get_json(self, key):
        o = self.client.get_object(Bucket=self.bucket_name, Key=key)
        
        if get('ContentEncoding', o, None) == 'gzip':
            v = gzip.decompress(o['Body'].read()).decode('utf-8')
        else:
            v = o['Body'].read().decode('utf-8')

        return json.loads(v)
            
                
    def _put_json(self, key, value, compress=True):

        """if compression is desired, this works:
        json.loads(gzip.decompress(gzip.compress(bytes(json.dumps({'a': 1}), 'utf-8'))))
 
        Just make sure to set the content encoding.
        """

        v = bytes(json.dumps(value), 'utf-8')

        if compress:
            v = gzip.compress(v)
            
            return self.bucket.put_object(Bucket=self.bucket_name,
                                          Key=key,
                                          Body=v,
                                          ACL='public-read',
                                          ContentType='application/json; charset=utf-8',
                                          ContentLength=len(v),
                                          ContentEncoding='gzip')
        else:
            return self.bucket.put_object(Bucket=self.bucket_name,
                                          Key=key,
                                          Body=v,
                                          ACL='public-read',
                                          ContentType='application/json; charset=utf-8',
                                          ContentLength=len(v))

    def _delete(self, key):
        return self.client.delete_object(Bucket=self.bucket_name, Key=key)

    def _tile_key(self, tx, ty):
        return 'tile/{tx}-{ty}.json'.format(tx=tx, ty=ty)

    def _chip_key(self, cx, cy):
        return 'chip/{cx}-{cy}.json'.format(cx=cx, cy=cy)

    def _pixel_key(self, cx, cy):
        return 'pixel/{cx}-{cy}.json'.format(cx=cx, cy=cy)

    def _segment_key(self, cx, cy):
        return 'segment/{cx}-{cy}.json'.format(cx=cx, cy=cy)

    def _prediction_key(self, cx, cy):
        return 'prediction/{cx}-{cy}.json'.format(cx=cx, cy=cy)
