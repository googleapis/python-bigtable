# Copyright 2018 Google LLC
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

"""This script is used to synthesize generated parts of this library."""

from pathlib import Path
import re
import textwrap
from typing import List, Optional

import synthtool as s
from synthtool import gcp, _tracked_paths
from synthtool.languages import python
from synthtool.sources import templates

common = gcp.CommonTemplates()

# This is a customized version of the s.get_staging_dirs() function from synthtool to
# cater for copying 2 different folders from googleapis-gen
# which are bigtable and bigtable/admin.
# Source https://github.com/googleapis/synthtool/blob/master/synthtool/transforms.py#L280
def get_staging_dirs(
    default_version: Optional[str] = None, sub_directory: Optional[str] = None
) -> List[Path]:
    """Returns the list of directories, one per version, copied from
    https://github.com/googleapis/googleapis-gen. Will return in lexical sorting
    order with the exception of the default_version which will be last (if specified).

    Args:
      default_version (str): the default version of the API. The directory for this version
        will be the last item in the returned list if specified.
      sub_directory (str): if a `sub_directory` is provided, only the directories within the
        specified `sub_directory` will be returned.

    Returns: the empty list if no file were copied.
    """

    staging = Path("owl-bot-staging")

    if sub_directory:
        staging /= sub_directory

    if staging.is_dir():
        # Collect the subdirectories of the staging directory.
        versions = [v.name for v in staging.iterdir() if v.is_dir()]
        # Reorder the versions so the default version always comes last.
        versions = [v for v in versions if v != default_version]
        versions.sort()
        if default_version is not None:
            versions += [default_version]
        dirs = [staging / v for v in versions]
        for dir in dirs:
            s._tracked_paths.add(dir)
        return dirs
    else:
        return []

# This library ships clients for two different APIs,
# BigTable and BigTable Admin
bigtable_default_version = "v2"
bigtable_admin_default_version = "v2"

for library in get_staging_dirs(bigtable_default_version, "bigtable"):
    s.move(library / "google/cloud/bigtable_v2", excludes=["**/gapic_version.py"])
    s.move(library / "tests")
    s.move(library / "scripts")

for library in get_staging_dirs(bigtable_admin_default_version, "bigtable_admin"):
    s.move(library / "google/cloud/bigtable/admin", excludes=["**/gapic_version.py"])
    s.move(library / "google/cloud/bigtable/admin_v2", excludes=["**/gapic_version.py"])
    s.move(library / "tests")
    s.move(library / "samples")
    s.move(library / "scripts")
    s.move(library / "docs/admin_v2", destination="docs/admin_client")

s.remove_staging_dirs()

# ----------------------------------------------------------------------------
# Add templated files
# ----------------------------------------------------------------------------
templated_files = common.py_library(
    samples=True,  # set to True only if there are samples
    split_system_tests=True,
    microgenerator=True,
    cov_level=99,
    system_test_external_dependencies=[
        "pytest-asyncio==0.21.2",
    ],
)

s.move(templated_files, excludes=[".coveragerc", "README.rst", ".github/release-please.yml", "noxfile.py"])

# ----------------------------------------------------------------------------
# Samples templates
# ----------------------------------------------------------------------------

python.py_samples(skip_readmes=True)

s.replace(
    "samples/beam/noxfile.py",
    """INSTALL_LIBRARY_FROM_SOURCE \= os.environ.get\("INSTALL_LIBRARY_FROM_SOURCE", False\) in \(
    "True",
    "true",
\)""",
    """# todo(kolea2): temporary workaround to install pinned dep version
INSTALL_LIBRARY_FROM_SOURCE = False""")

# --------------------------------------------------------------------------
# Admin Overlay work
# --------------------------------------------------------------------------

# Add overlay imports to top level __init__.py files in admin_v2 and admin at the end
# of each file, after the __all__ definition.
def add_overlay_to_init_py(init_py_location, import_statements):
    s.replace(
        init_py_location,
        r"(?s)(^__all__ = \(.*\)$)",
        r"\1\n\n" + import_statements
    )

add_overlay_to_init_py(
    "google/cloud/bigtable/admin_v2/__init__.py",
    """from .overlay import *  # noqa: F403
__all__ += overlay.__all__  # noqa: F405
"""
)

add_overlay_to_init_py(
    "google/cloud/bigtable/admin/__init__.py",
    """import google.cloud.bigtable.admin_v2.overlay  # noqa: F401
from google.cloud.bigtable.admin_v2.overlay import *  # noqa: F401, F403

__all__ += google.cloud.bigtable.admin_v2.overlay.__all__
"""
)

# Replace all instances of BaseBigtableTableAdminClient/BaseBigtableAdminAsyncClient
# in samples and docstrings with BigtableTableAdminClient/BigtableTableAdminAsyncClient
s.replace(
    [
        "google/cloud/bigtable/admin_v2/services/*/client.py",
        "google/cloud/bigtable/admin_v2/services/*/async_client.py",
        "samples/generated_samples/bigtableadmin_v2_*.py"
    ],
    r"client = admin_v2\.Base(BigtableTableAdmin(Async)?Client\(\))",
    r"client = admin_v2.\1"
)

# Fix an improperly formatted table that breaks nox -s docs.
s.replace(
    "google/cloud/bigtable/admin_v2/types/table.py",
    """            For example, if \\\\_key =
            "some_id#2024-04-30#\\\\x00\\\\x13\\\\x00\\\\xf3" with the following
            schema: \\{ fields \\{ field_name: "id" type \\{ string \\{
            encoding: utf8_bytes \\{\\} \\} \\} \\} fields \\{ field_name: "date"
            type \\{ string \\{ encoding: utf8_bytes \\{\\} \\} \\} \\} fields \\{
            field_name: "product_code" type \\{ int64 \\{ encoding:
            big_endian_bytes \\{\\} \\} \\} \\} encoding \\{ delimited_bytes \\{
            delimiter: "#" \\} \\} \\}

            \\| The decoded key parts would be: id = "some_id", date =
              "2024-04-30", product_code = 1245427 The query "SELECT
              \\\\_key, product_code FROM table" will return two columns:
              /------------------------------------------------------
            \\| \\\\\\| \\\\_key \\\\\\| product_code \\\\\\| \\\\\\|
              --------------------------------------\\|--------------\\\\\\| \\\\\\|
              "some_id#2024-04-30#\\\\x00\\\\x13\\\\x00\\\\xf3" \\\\\\| 1245427 \\\\\\|
              ------------------------------------------------------/
""",
    textwrap.indent(
        """For example, if \\\\_key =
"some_id#2024-04-30#\\\\x00\\\\x13\\\\x00\\\\xf3" with the following
schema:

.. code-block::

    {
      fields {
        field_name: "id"
        type { string { encoding: utf8_bytes {} } }
      }
      fields {
        field_name: "date"
        type { string { encoding: utf8_bytes {} } }
      }
      fields {
        field_name: "product_code"
        type { int64 { encoding: big_endian_bytes {} } }
      }
      encoding { delimited_bytes { delimiter: "#" } }
    }

The decoded key parts would be:
id = "some_id", date = "2024-04-30", product_code = 1245427
The query "SELECT \\\\_key, product_code FROM table" will return
two columns:

+========================================+==============+
| \\\\_key                                  | product_code |
+========================================+==============+
| "some_id#2024-04-30#\\\\x00\\\\x13\\\\x00\\\\xf3"  |    1245427   |
+----------------------------------------+--------------+
""",
    " " * 12,
    ),
)

# Change the subpackage for clients with overridden internal methods in them
# from service to overlay.service.
s.replace(
    "docs/admin_client/bigtable_table_admin.rst",
    r"^\.\. automodule:: google\.cloud\.bigtable\.admin_v2\.services\.bigtable_table_admin$",
    ".. automodule:: google.cloud.bigtable.admin_v2.overlay.services.bigtable_table_admin"
)

# Add overlay types to types documentation
s.replace(
    "docs/admin_client/types_.rst",
    r"""(\.\. automodule:: google\.cloud\.bigtable\.admin_v2\.types
    :members:
    :show-inheritance:)
""",
    r"""\1

.. automodule:: google.cloud.bigtable.admin_v2.overlay.types
    :members:
    :show-inheritance:
"""
)

# Oneofs work:

# Move the definition of a oneof message into the autogenerated types
# directory. This is needed to prevent circular import issues in admin_v2/overlay.
# This can also be used to insert other files into other autogenerated directories.
gapic_templates = templates.TemplateGroup("gapic_templates")
_tracked_paths.add(gapic_templates.dir)
gapic_templated_files = gapic_templates.render()
s.move(
    gapic_templated_files,
    destination="google/cloud/bigtable",
    excludes=["README.rst"],
)

# Add the oneof_message import into table.py for GcRule
s.replace(
    "google/cloud/bigtable/admin_v2/types/table.py",
    r"^(from google\.cloud\.bigtable\.admin_v2\.types import .+)$",
    r"""\1
from google.cloud.bigtable.admin_v2.types import oneof_message""",
)

# Re-subclass GcRule in table.py
s.replace(
    "google/cloud/bigtable/admin_v2/types/table.py",
    r"class GcRule\(proto\.Message\)\:",
    "class GcRule(oneof_message.OneofMessage):",
)

s.shell.run(["nox", "-s", "blacken"], hide_output=False)
