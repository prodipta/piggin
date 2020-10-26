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

import os
import re
import logging

from piggin.s3.s3 import AwsS3

logger = logging.getLogger('piggin')
logger.setLevel(logging.INFO)

def copy_from_s3(src, dest, pattern, recursive=False, access_key=None, 
               secret_key=None, profile=None):
    """
        Copy files from s3 source to local file system.
        
        Args:
            `src (str)`: S3 bucket and key path name.
            
            `dest (src)`: Local file system target.
            
            `pattern (str)`: Pattern to match.
            
            `recursive (bool)`: If search directories recursively.
            
            `access_key (str)`: AWS access key.
            
            `secret_key (str)`: AWS secret key.
            
            `profile (str)`: AWS profile name.
            
        Returns:
            None. Copies the files from source to destination.
    """
    awsS3 = AwsS3(access_key, secret_key, profile)
    
    try:
        _type, bucket, key, _ = awsS3.parse_path('s3:'+src)
    except:
        _type, bucket, key, _ = awsS3.parse_path(src)
        
    if _type != 's3':
        raise ValueError('source must be an S3 location.')
        
    _type, _, _, path = awsS3.parse_path(dest)
    if _type == 's3':
        raise ValueError('destination must be a local fs location.')
    
    if not os.path.exists(path):
        raise ValueError('destination location does not exist.')
        
    if not os.path.isdir(path):
        raise ValueError('destination location must be a directory.')
        
    if pattern:
        p = re.compile(pattern)
    else:
        p = re.compile('.')
        
    files = awsS3.ls(src)
    
    try:
        for file in files:
            print(f'processing file {file} in {bucket}.')
            if file == src:
                continue
            if file.endswith('/'):
                print(f'calling recursively...')
                if recursive:
                    copy_from_s3(
                            's3:///'+bucket+'/'+file, dest, pattern, 
                            recursive=recursive, access_key=access_key, 
                            secret_key=secret_key, profile=profile)
                continue
            
            name = file.split('/')[-1]
            if not re.search(p, name):
                continue
            
            source = 's3:///'+bucket+'/'+file
            target = os.path.join(dest, name)
            awsS3.copy(source, target)
            print(f'copied file {source} to {target}.')
    except:
        logger.error(f'failed copying from s3:{file}')
        raise