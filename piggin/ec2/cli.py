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

CONTEXT_SETTINGS = dict(ignore_unknown_options=True,
                        allow_extra_args=True,
                        token_normalize_func=lambda x: x.lower())

@click.group()
@click.pass_context
def ec2(ctx):
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
    pass

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
    click.echo(f'handling ec2 ls command {verbose}')
    click.echo(f'{ctx.obj}')