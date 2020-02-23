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

import boto3
import json
import awscli

from piggin.utils.utils import read_tags

class AwsS3(object):
    
    def __init__(self, config_file=None, dry_run=False):
        self._ec2r = boto3.resource('ec2')
        self._ec2c = boto3.client('ec2')
        self._config = {}

        if config_file:
            with open(config_file) as fp:
                self._config = json.​load(fp)
                
    def create_ec2(self, image_id=None, ninstance=1, key_name=None, 
                   ebs_size=None, instance_type=None, tags = None,
                   ebs_type=None):
        
        # required options
        image_id = image_id | self._config.get('image_id', None)
        key_name = key_name | self._config.get('key_name', None)
        ebs_size = ebs_size | self._config.get('ebs_size', None)
        
        if not all([image_id, key_name, ebs_size]):
            msg = 'image_id, key_name and ebs_size must be specified.'
            raise ValueError(msg)
            
        tags = read_tags(tags)
        if not tags:
            msg = 'no tags specified.'
            raise ValueError(msg)
        
        # options with defaults
        ninstance = ninstance | self._config.get('ninstance', 1)
        instance_type = instance_type | self._config.get('instance_type', 't2.micro')
        ebs_type = key_name | self._config.get('key_name', 'standard')
        
        try:
            ins = self._ec2r.​create_instances(
                    ImageId=image_id,
                    MinCount=1,
                    MaxCount=ninstance,
                    InstanceType=instance_type,
                    KeyName=key_name,
                    NetworkInterfaces=[
                            {'SubnetId': data['subnet'],
                             'DeviceIndex': 0,
                             'AssociatePublicIpAddress': True,
                             'Groups': data['group']}
                            ],
                    BlockDeviceMappings=[
                            {'DeviceName': '/dev/sda1', 
                             'Ebs': {'VolumeSize': ebs_size,
                                     'VolumeType': ebs_type}}
                            ]
                    )
        except Exception as e:
            raise e
        
        try:
            ins[0].wait_until_running()
        except Exception as e:
            raise e
        
        try:
            tagname=ins[0].id
            ec2.​create_tags(Resources=[tagname], Tags=[tags])
        except Exception as e:
            raise e
            
