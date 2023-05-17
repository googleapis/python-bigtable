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
#


class TestSyncUpToDate:
    """
    Unit tests should fail if the sync surface is out of date from async

    To re-generate the sync surface, run the following command from the repo root:
    python3 sync_surface_generator
    """

    def test_code_up_to_date(self):
        import sys

        sys.path.append("../../../../")
        import sync_surface_generator
        import inspect
        from google.cloud.bigtable._sync import _autogen

        # generate a new copy of the sync surface
        generated_code = sync_surface_generator.generate_full_surface()
        # load the current saved sync surface
        filename = inspect.getfile(_autogen)
        saved_code = open(filename, "r").read()
        # check if the surfaces differ
        assert (
            generated_code == saved_code
        ), "Sync surface is not up to date, and needs to be re-generated"



    def test_system_tests_up_to_date(self):
        import sys

        sys.path.append("../../../../")
        import sync_surface_generator
        import inspect
        from tests.system import test_system_sync_autogen

        # generate a new copy of the sync tests
        generated_code = sync_surface_generator.generate_tests()
        # load the current saved sync tests
        filename = inspect.getfile(test_system_sync_autogen)
        saved_code = open(filename, "r").read()
        # check if the surfaces differ
        assert (
            generated_code == saved_code
        ), "Sync system tests are not up to date, and need to be re-generated"
