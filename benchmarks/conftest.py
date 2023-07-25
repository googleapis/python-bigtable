# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Import pytest fixtures for setting up table from system test directory.
"""
import sys
import os

script_path = os.path.dirname(os.path.realpath(__file__))
system_test_path = os.path.join(script_path, "..", "tests", "system", "data")
sys.path.append(system_test_path)

pytest_plugins = [
    "setup_fixtures",
]
