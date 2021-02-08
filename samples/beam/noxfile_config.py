# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Default TEST_CONFIG_OVERRIDE for python repos.

# You can copy this file into your directory, then it will be imported from
# the noxfile.py.

# The source of truth:
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/noxfile_config.py

TEST_CONFIG_OVERRIDE = {
  # A dictionary you want to inject into your test. Don't put any
  # secrets here. These values will override predefined values.
  "envs": {"INSTALL_LIBRARY_FROM_SOURCE": "False"},
}
