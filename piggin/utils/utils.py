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
import json
import re


def read_tags(tag):
    """
        read a list of tags, either from a json file or a list of comma 
        separated key=value pairs.
    """
    if os.path.isfile(tag):
        with open(tag) as fp:
            tags = json.load(fp)
    elif os.path.isfile(os.path.join(os.path.expanduser("~"),tag)):
        target = os.path.isfile(os.path.join(os.path.expanduser("~"),tag))
        with open(target) as fp:
            tags = json.load(fp)
    elif isinstance(tag, str):
        values = re.split(';|,|\n',tag)
        if values:
            print(values)
            tags = {}
            for item in values:
                if '=' not in item:
                    raise ValueError(f'illegal tag {item}')
                key, value = tuple(item.split('='))
                tags[key] = value
                
    return tags
        
        
                