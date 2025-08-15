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
import warnings

_new_path = "google.cloud.bigtable.admin"
_old_path = __name__  # This will be 'google.cloud.bigtable_admin_v2'

# Issue a warning to users that the old path is deprecated.
warnings.warn(
    f"The '{_old_path}' package is deprecated and will be removed in a future "
    f"version. Please use '{_new_path}' instead.",
    DeprecationWarning,
    stacklevel=2,
)

# 1. Import the real top-level package from its new location.
_real_package = importlib.import_module(_new_path)

# 2. Immediately replace this alias package in the module cache with the real one.
sys.modules[_old_path] = _real_package

# 3. Eagerly walk and import all submodules of the real package.
#    This populates sys.modules with all the canonical submodule paths
#    (e.g., 'google.cloud.bigtable.admin.types', etc.).
for _, name, _ in pkgutil.walk_packages(
    path=_real_package.__path__,
    prefix=_real_package.__name__ + ".",
):
    importlib.import_module(name)

# 4. Now that they're all loaded, create an alias for each submodule.
#    We iterate over a copy of the keys, as the dictionary size will change.
for name in list(sys.modules.keys()):
    # Check if a loaded module is a submodule of our new package...
    if name.startswith(_new_path + "."):
        # ...and create a corresponding alias using the old path prefix.
        old_submodule_path = name.replace(_new_path, _old_path, 1)
        sys.modules[old_submodule_path] = sys.modules[name]