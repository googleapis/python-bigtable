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
# limitations under the License

import importlib
import sys

_CLASSIC_CLIENT_MODULE_NAMES = {
     "app_profile",
    "backup",
    "batcher",
    "client",
    "cluster",
    "column_family",
    "encryption_info",
    "enums",
    "error",
    "helpers",
    "instance",
    "policy",
    "row_data",
    "row_filters",
    "row_merger",
    "row_set",
    "row",
    "table",
}

# update sys.modules to add an alias from old path to new
for module_name in _CLASSIC_CLIENT_MODULE_NAMES:
    old_path = f"google.cloud.bigtable.{module_name}"
    new_path = f".classic.{module_name}"

    module = importlib.import_module(new_path, __name__)
    sys.modules[old_path] = module
    # Attach the module as an attribute to the current package
    setattr(sys.modules[__name__], module_name, module)

# previously exported classes
from google.cloud.bigtable.classic.client import Client
from . import gapic_version
__version__ = gapic_version.__version__