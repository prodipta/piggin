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

from piggin.utils.types import HashKeyType
from piggin.s3.cli import s3
from piggin.ec2.cli import ec2

CONTEXT_SETTINGS = dict(ignore_unknown_options=True,
                        allow_extra_args=True,
                        token_normalize_func=lambda x: x.lower())
    
@click.group()
@click.option(
    '--access-key', 
    '-a',
    envvar="AWS_ACCESS_KEY_ID",
    default=None,
    type=HashKeyType(length=20),
    help='your AWS API key'
)
@click.option(
    '--secret-key',
    '-s',
    envvar="AWS_SECRET_ACCESS_KEY",
    default=None,
    type=HashKeyType(length=0),
    help='your AWS secret key'
)
@click.option(
    '--profile-name',
    '-p',
    envvar="AWS_PROFILE",
    default=None,
    help='your AWS profile name'
)
@click.pass_context
def main(ctx, access_key, secret_key, profile_name):
    """
        piggin is command line utility program to interact with 
        AWS resources.
        
        Usage:\n
            piggin --help\n
            piggin s3 subcommand [options]\n
            piggin ec2 subcommand [options]\n
    """
    ctx.obj = {
               'access_key': access_key,
               'secret_key': secret_key,
               'profile_name':profile_name,
               }
    
main.add_command(s3)
main.add_command(ec2)

if __name__ == "__main__":
    main()