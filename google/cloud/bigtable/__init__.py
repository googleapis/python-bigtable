import importlib
import sys
import warnings


_LEGACY_MODULE_NAMES = {
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

# Eagerly create aliases in sys.modules for full backward compatibility.
# This is more robust than the lazy __getattr__ approach because it handles
# direct submodule imports (e.g., `import google.cloud.bigtable.row_data`).
for module_name in _LEGACY_MODULE_NAMES:
    # Define the old and new module paths
    old_path = f"google.cloud.bigtable.{module_name}"
    new_path = f".classic.{module_name}"

    # Import the module from its new location
    module = importlib.import_module(new_path, __name__)

    # Inject the loaded module into sys.modules under its old path.
    # This makes Python's import machinery find it automatically.
    sys.modules[old_path] = module

    # Attach the module as an attribute to the current package
    setattr(sys.modules[__name__], module_name, module)

# You can also issue a single, general warning that some modules were moved.
warnings.warn(
    (
        "Several modules have been moved to the `google.cloud.bigtable.classic` "
        "subpackage. The top-level aliases will be removed in a future version."
    ),
    DeprecationWarning,
    stacklevel=2,
)

# previously exported classes
from google.cloud.bigtable.classic.client import Client
from . import gapic_version
__version__ = gapic_version.__version__