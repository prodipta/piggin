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

from piggin.s3.s3 import AwsS3

def copy_from_s3(src, dest, pattern, access_key=None, 
               secret_key=None, profile=None):
    """
        Copy files from s3 source to local file system.
        
        Args:
            `src (str)`: S3 bucket and key path name.
            
            `dest (src)`: Local file system target.
            
            `pattern (str)`: Pattern to match.
            
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
        
    files = awsS3.ls(src)
    
    for file in files:
        if file == src:
            continue
        if file.endswith('/'):
            continue
        
        name = file.split('/')[-1]
        if pattern is not None:
            if pattern not in name:
                continue
        
        source = 's3:///'+bucket+'/'+file
        target = os.path.join(dest, name)
        awsS3.copy(source, target)