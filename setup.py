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
from setuptools import setup, find_packages

__package_name__ = "piggin"
setup_path = os.path.abspath(os.path.dirname(__file__))
versioneer = "version.py"

# read versioning
namespace = {}
with open(os.path.join(setup_path, __package_name__, versioneer)) as fp:
    code = compile(fp.read(), versioneer, 'exec')
    exec(code, namespace)

# read requirements.txt for dependencies
def parse_requirements(requirements_txt):
    with open(requirements_txt) as f:
        for line in f.read().splitlines():
            if not line or line.startswith("#"):
                continue
            yield line
            
def install_requires():
    return list(set([r for r in parse_requirements('requirements.txt')]))

setup(
    name=__package_name__,
    url=namespace["__url__"],
    version=namespace["__version__"],
    description=namespace["__description__"],
    long_description=namespace["__long_desc__"],
    entry_points={'console_scripts': ['piggin = piggin.__main__:main']},
    author=namespace["__author__"],
    author_email=namespace["__email__"],
    packages=find_packages(include=["piggin", 
                                    "piggin.*"]),
    include_package_data=True,
    license=namespace["__license__"],
    classifiers=namespace["__package_classifier__"],
    install_requires=install_requires()
)