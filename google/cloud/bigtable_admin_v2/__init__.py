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

_new_path = "google.cloud.bigtable.admin"

# update sys.modules to add an alias from this path to new
_real_package = importlib.import_module(_new_path)
sys.modules[__name__] = _real_package

# iterate and import all submodules to populate sys.modules
for _, name, _ in pkgutil.walk_packages(
    path=_real_package.__path__,
    prefix=_real_package.__name__ + ".",
):
    importlib.import_module(name)

# create an alias for each submodule
for name in list(sys.modules.keys()):
    if name.startswith(_new_path + "."):
        old_submodule_path = name.replace(_new_path, __name__, 1)
        sys.modules[old_submodule_path] = sys.modules[name]