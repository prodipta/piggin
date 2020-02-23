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
from datetime import datetime
from pytz import all_timezones as pytz_all_timezones
import re
import pandas as pd
import os

class HashKeyType(click.ParamType):
    name = 'SHA KEY'
    def __init__(self, length=32):
        super(HashKeyType, self).__init__()
        self.length = length

    def convert(self, value, param, ctx):
        if self.length == 0:
            return value
        
        pattern = "^[0-9a-zA-Z]{"+str(self.length)+"}$"
        matched = re.match(pattern, value)
        if not matched:
            self.fail(f'{value} is not a valid SHA/ MD5 key', param, ctx)
        return value

class TimezoneType(click.ParamType):
    name = 'Standard time zone names'
    def convert(self, value, param, ctx):
        valid = str(value) in pytz_all_timezones
        if not valid:
            self.fail(f'{value} is not a valid time zone', param, ctx)
        return value

class DateType(click.ParamType):
    name = 'Date input'
    def __init__(self):
        strformats = ['%Y-%m-%d', '%d-%b-%Y', '%Y-%b-%d']
        self.formats = strformats
        super(DateType, self).__init__()

    def _try_to_convert_date(self, value, format):
        try:
            return datetime.strptime(value, format)
        except ValueError:
            return None

    def convert(self, value, param, ctx):
        for format in self.formats:
            dt = self._try_to_convert_date(value, format)
            if dt:
                return pd.Timestamp(dt)

        self.fail(
            'invalid datetime format: {}. (choose from {})'.format(
                value, ', '.join(self.formats)))
        
class OSEnvAwsReset():
    '''
        A context manager for handling AWS keys environment variable
        update.
    '''
    
    def __init__(self, access_key, secret_key):
        self._access_key = access_key
        self._secret_key = secret_key
        self._old_access_key = None
        self._old_secret_key = None
    
    def __enter__(self):
        if self._access_key is None or self._secret_key is None:
            return self
        
        self._old_access_key = os.environ.get('AWS_ACCESS_KEY_ID', None)
        self._old_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
        os.environ['AWS_ACCESS_KEY_ID']=self._access_key
        os.environ['AWS_SECRET_ACCESS_KEY']=self._secret_key
        return self
    
    def __exit__(self, *args):
        if self._old_access_key:
            os.environ['AWS_ACCESS_KEY_ID']=self._old_access_key
        
        if self._old_secret_key:
            os.environ['AWS_SECRET_ACCESS_KEY']=self._old_secret_key

        