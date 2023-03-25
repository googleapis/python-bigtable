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


import pytest
import unittest


class TestBigtableDataClient(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        pass

    def test_channel_pool_creation(self):
        pass

    def test_channel_pool_rotation(self):
        pass

    def test_channel_pool_replace(self):
        pass

    def test_start_background_channel_refresh(self):
        pass

    def test__ping_and_warm_instances(self):
        pass

    def test__manage_channel(self):
        pass

    def test_register_instance(self):
        pass

    def test_remove_instance_registration(self):
        pass

    def test_get_table(self):
        pass


class TestTable(unittest.TestCase):


    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient().get_table(*args, **kwargs)

    def test_ctor(self):
        pass
