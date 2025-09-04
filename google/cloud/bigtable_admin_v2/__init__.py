# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
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
#

import sys
import importlib
import pkgutil

import google.cloud.bigtable.admin_v2

# Alias all subpackages of google.cloud.bigtable.admin_v2 to
# corresponding subpackages of google.cloud.bigtable_admin_v2.

_NEW_PATH = "google.cloud.bigtable.admin_v2"
sys.modules[__name__] = google.cloud.bigtable.admin_v2

# iterate and import all submodules to populate sys.modules
for _, name, _ in pkgutil.walk_packages(
    path=google.cloud.bigtable.admin_v2.__path__,
    prefix=_NEW_PATH + ".",
):
    mod = importlib.import_module(name)
    alias = name.replace(_NEW_PATH, __name__, 1)
    sys.modules[alias] = mod
