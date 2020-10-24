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
import logging

from piggin.common.utils import read_tags

class AwsEC2(object):
    
    def __init__(self, access_key=None, secret_key=None, 
                 profile_name=None, region=None):
        session = boto3.Session(aws_access_key_id=access_key,
                                aws_secret_access_key=secret_key,
                                profile_name=profile_name)
        
        if session.region_name is None:
            self._default_region = region
        else:
            self._default_region = session.region_name
        
        self._ec2r = session.resource('ec2', region_name=self._default_region)
        self._ec2c = session.client('ec2', region_name=self._default_region)
        self._logger = logging.getLogger('ec2')

    def create_ec2(self, image_id=None, ninstance=1, key_name=None, 
                   ebs_size=None, instance_type=None, tags = None,
                   ebs_type=None, config=None):
        
        if config:
            with open(config) as fp:
                data = json.load(fp)
        
        # required options
        image_id = image_id | data.get('image_id', None)
        key_name = key_name | data.get('key_name', None)
        ebs_size = ebs_size | data.get('ebs_size', None)
        
        if not all([image_id, key_name, ebs_size]):
            msg = 'image_id, key_name and ebs_size must be specified.'
            raise ValueError(msg)
            
        tags = read_tags(tags)
        if not tags:
            msg = 'no tags specified.'
            raise ValueError(msg)
        
        # options with defaults
        ninstance = ninstance | data.get('ninstance', 1)
        instance_type = instance_type | data.get('instance_type', 't2.micro')
        ebs_type = key_name | data.get('key_name', 'standard')
        
        try:
            ins = self._ec2r.create_instances(
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
            self._ec2r.create_tags(Resources=[tagname], Tags=[tags])
        except Exception as e:
            raise e
            
    def ls(self):
        instances = self._ec2r.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
        return instances
