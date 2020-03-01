# Copyright 2020 QuantInsti Quantitative Learnings Pvt Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import boto3

from piggin.utils.utils import confirm_action

class AwsS3(object):
    
    def __init__(self, access_key=None, secret_key=None, profile_name=None):
        self._access_key = access_key
        self._secret_key = secret_key
        session = boto3.Session(aws_access_key_id=access_key,
                                aws_secret_access_key=secret_key,
                                profile_name=profile_name)
        self._s3r = session.resource('s3')
        self._s3c = session.client('s3')
        self._logger = logging.getLogger('s3')
    
    def mkbucket(self, bucket_name, acl, location):
        self.create_bucket(bucket_name, acl=acl, location=location)                
            
    def mkdir(self, strpath):
        protocol, bucket, key, path = self.parse_path(strpath)
        if protocol == 's3':
            if not key.endswith('/'):
                key = key+'/'
            self.touch(bucket, key)
    
    def copy(self, str_src, str_dest):
        protocol1, bucket1, key1, path1 = self.parse_path(str_src)
        protocol2, bucket2, key2, path2 = self.parse_path(str_dest)
        
        if protocol1 == 's3' and protocol2 == 'file':
            self.download(bucket1, key1, path2)
        elif protocol1 == 'file' and protocol2 == 's3':
            self.upload(bucket2,key2,path1)
        else:
            raise('Unknown source {} or destination {}'.format(str_src, str_dest))
    
    def rm(self, str_path, confirm=True, recursive=False):
        if not str_path.startswith('s3:'):
            str_path = 's3:///' + str_path
            
        protocol, bucket, key, path = self.parse_path(str_path)
        
        if protocol != 's3':
            self._logger.error('Path {} not an s3 object'.format(str_path))
            return
        
        if bucket == '' or bucket is None:
            self._logger.error('missing bucket name.')
            return
        
        if not recursive:
            tmp_key = key
            if key != '' and not key.endswith('/'):
                tmp_key = key+'/'
            objects = self.list_objects(bucket, tmp_key)
            if tmp_key in objects:
                objects.remove(tmp_key)
            if objects:
                msg = 'cannot delete, not empty.'
                self._logger.error(msg)
                return
        
        if key == '':
            if confirm:
                msg = f'are you sure to delete bucket {bucket}'
                response = confirm_action(msg)
                if not response:
                    return
            self.delete_bucket(bucket)
        else:
            if confirm:
                msg = f'are you sure to delete {key} in {bucket}'
                response = confirm_action(msg)
                if not response:
                    return
            self.delete_objects(bucket, key)
    
    def ls(self, str_path):
        if not str_path.startswith('s3:'):
            str_path = 's3:///' + str_path
        
        protocol, bucket, key, path = self.parse_path(str_path)
        if protocol == 's3':
            if bucket:
                return self.list_objects(bucket, key)
            else:
                return self.ls_bucket()
        else:
            raise ValueError(f'illegal path {str_path}')
            
    def ls_bucket(self):
        try:
            response = self._s3c.list_buckets()
            buckets = [b['Name'] for b in response['Buckets']]
            return buckets
        except Exception as e:
            self._logger.error(str(e))
            
    def list_objects(self, bucket, key):     
        if bucket == '' or bucket is None:
            self._logger.error('missing bucket name.')
            return
        
        try:
            objects = self._list_s3_objects(bucket, key)
            return [o for o in objects]
        except Exception as e:
            msg = f'listing {key} in {bucket}:'+str(e)
            self._logger.error(msg)
    
    def upload(self, bucket_name, key, file_name):
        with open(file_name, 'rb') as fd:
            try:
                self._s3r.Object(bucket_name, key).put(Body=fd)
            except Exception as e:
                self._logger.error(str(e))
                
    def touch(self, bucket_name, key):
        if bucket_name == '' or bucket_name is None:
            self._logger.error('missing bucket name.')
            return
        
        if key == '' or key is None:
            self._logger.error('missing key name.')
            return
        
        try:
            self._s3r.Object(bucket_name, key).put(Body='')
        except Exception as e:
            self._logger.error(str(e))
    
    def download(self, bucket_name, key, file_name):
        try:
            self._s3r.Bucket(bucket_name).download_file(key, file_name)
        except Exception as e:
            self._logger.error(str(e))
    
    def delete_object(self, bucket_name, key):
        if bucket_name == '' or bucket_name is None:
            self._logger.error('missing bucket name.')
            return
        
        if key == '' or key is None:
            self._logger.error('missing key name.')
            return
        
        try:
            obj = self._s3r.Object(bucket_name, key)
            obj.delete()
        except Exception as e:
            msg = f'deleting {key} in {bucket_name}:'+str(e)
            self._logger.error(msg)
            
    def delete_objects(self, bucket_name, key):
        if bucket_name == '' or bucket_name is None:
            self._logger.error('missing bucket name.')
            return
        
        if key == '' or key is None:
            self._logger.error('missing key name.')
            return
        
        try:
            bucket = self._s3r.Bucket(bucket_name)
            bucket.objects.filter(Prefix=key).delete()
            self.delete_object(bucket_name, key)
        except Exception as e:
            msg = f'deleting all under {key} in {bucket_name}:'+str(e)
            self._logger.error(msg)
    
    def create_bucket(self, bucket_name, *args, **kwargs ):
        if bucket_name == '' or bucket_name is None:
            self._logger.error('missing bucket name.')
            return
        
        location = kwargs.pop('location', 'us-west-1')
        acl = kwargs.pop('acl', 'private')
        
        if not location:
            location = 'us-west-1'
        if not acl:
            acl = 'private'
        
        acl = acl.strip('\"').strip("\'")
        location = location.strip('\"').strip("\'")
        
        try:
            self._s3r.create_bucket(Bucket=bucket_name, ACL = acl,
            CreateBucketConfiguration={'LocationConstraint':location})
        except Exception as e:
            self._logger.error(str(e))
            
    def delete_bucket(self, bucket_name):
        if bucket_name == '' or bucket_name is None:
            self._logger.error('missing bucket name.')
            return
        try:
            bucket = self._s3r.Bucket(bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except Exception as e:
            self._logger.error(str(e))
            
    @classmethod
    def parse_path(cls, str_path):
        parts = str_path.split(':')

        if len(parts) == 2:
            protocol = parts[0]
            if protocol.lower() == 's3':
                parts[1] = parts[1].lstrip('/')
                bucket = parts[1].split('/')[0]
                key = parts[1][len(bucket)+1:]
                return 's3', bucket, key, None
            else:
                return 'file', None, None, str_path
        elif len(parts) == 1:
            return 'file', None, None, parts[0]
        else:
            raise('Illegal path name: {}!'.format(str_path))
        
    def _list_s3_objects(self, bucket_name, key):
        results = self._s3c.list_objects(
                Bucket=bucket_name,Prefix=key, Delimiter='/')
        
        for r in results.get('Contents',[]):
            yield(r.get('Key'))
            
        for r in results.get('CommonPrefixes', []):
            yield(r.get('Prefix'))
    
