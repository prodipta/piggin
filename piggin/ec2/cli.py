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


import click
from piggin.ec2.ec2 import AwsEC2
from piggin.utils.types import OSEnvAwsReset

CONTEXT_SETTINGS = dict(ignore_unknown_options=True,
                        allow_extra_args=True,
                        token_normalize_func=lambda x: x.lower())

@click.group()
@click.option(
    '--region',
    '-r',
    default='us-east-1',
    help='AWS region.')
@click.pass_context
def ec2(ctx, region):
    """
        piggin ec2 commands to interact with AWS ec2 resources.
        
        Usage:\n
            piggin --help\n
            piggin ec2 ls [options]\n
            piggin ec2 create [options]\n
            piggin ec2 tag [options]\n
            piggin ec2 rm [options]\n
            piggin ec2 update [options]\n
    """
    ctx.obj['region'] = region

@ec2.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    '--verbose/--silent',
    default=False,
    help='Turn on/ off verbosity. [verbose/silent]')
@click.pass_context
def ls(ctx, verbose):
    """
        list ec2 instances.
    """
    access_key = ctx.obj['access_key']
    secret_key = ctx.obj['secret_key']
    profile_name = ctx.obj['profile_name']
    region = ctx.obj['region']
    count = 0
    
    with OSEnvAwsReset(access_key, secret_key):
        ec2 = AwsEC2(access_key, secret_key, profile_name, region)
        instances = ec2.ls()
        
        msg = f'{"instance id":20}'
        msg = f'{"instance name":20}'
        msg = msg + f'{"instance_type":12}'
        msg = msg + f'{"key_name":20}'
        msg = msg + f'{"private_ip_address":16}'
        msg = msg + f'{"public_dns_name":25}'
        print(msg)
        print('_'*115)
        for instance in instances:
            msg = f'{instance.id:20}'
            msg = f'{instance.tags[0].get("Value"):20}'
            msg = msg + f'{instance.instance_type:12}'
            msg = msg + f'{instance.key_name:20}'
            msg = msg + f'{instance.private_ip_address:16}'
            msg = msg + f'{instance.public_dns_name:25}'
            print(msg)
            count += 1
    
    print('_'*115)
    print(f'total running instances {count}')