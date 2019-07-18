from blackmagic.data import Storage
from interface import implements

import boto3
import json
import gzip

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

keys = {'tile':       lambda tx, ty: 'tile-{tx}-{ty}.xgboost'.format(tx=tx, ty=ty),
        'chip':       lambda cx, cy: 'chip-{cx}-{cy}.json'.format(cx=cx, cy=cy),
        'pixel':      lambda cx, cy: 'pixel-{cx}-{cy}.json'.format(cx=cx, cy=cy),
        'segment':    lambda cx, cy: 'segment-{cx}-{cy}.json'.format(cx=cx, cy=cy),
        'prediction': lambda cx, cy: 'prediction-{cx}-{cy}.json'.format(cx=cx, cy=cy)}
    
prefixes = {'tile':       lambda h, v: '/tile/{h}/{v}'.format(h=h, v=v),
            'chip':       lambda h, v: '/chip/{h}/{v}'.format(h=h, v=v),
            'pixel':      lambda h, v: '/pixel/{h}/{v}'.format(h=h, v=v),
            'segment':    lambda h, v: '/segment/{h}/{v}'.format(h=h, v=v),
            'prediction': lambda h, v: '/prediction/{h}/{v}'.format(h=h, v=v)}


class Ceph(implements(Storage)):

    def __init__(self, url, access_key, secret_key, bucket_name):
        self.url = url
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        
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

    def get_bin(self, prefix, key):
        o = self.bucket.get_object(Bucket=self.bucket_name, Key=key)

        if o['ContentEncoding'] == 'gzip':
            v = gzip.decompress(o['Body'].read())
        else:
            v = o['Body'].read().decode('utf-8')

        return v

    def put_bin(self, prefix, key, value, compress=True):

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
    
    def get_json(self, prefix, key):
        o = self.bucket.get_object(Bucket=self.bucket_name, Key=key)

        if o['ContentEncoding'] == 'gzip':
            v = gzip.decompress(o['Body'].read()).decode('utf-8')
        else:
            v = o['Body'].read().decode('utf-8')

        return json.loads(v)
            
                
    def put_json(self, prefix, key, value, compress=True):

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

    def delete(self, prefix, key):
        return self.client.delete_object(Bucket=self.bucket_name, Key=key)


    
