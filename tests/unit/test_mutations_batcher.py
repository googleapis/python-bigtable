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

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore


class Test_FlowControl:
    def _make_one(self, table=None, max_mutation_count=10, max_mutation_bytes=100):
        from google.cloud.firestore_v1.batch import _FlowControl

        if table is None:
            table = mock.Mock()
        return _FlowControl(table, max_mutation_count, max_mutation_bytes)

    def test_ctor(self):
        pass

    def test_has_capacity(self, existing_size, new_size, existing_count, new_count, expected):
        pass

    def test__on_mutation_entry_complete(self):
        pass

    def test__execute_mutate_rows(self):
        pass

    def test_process_mutations(self):
        pass


class TestMutationsBatcher:

    def _make_one(self, table=None, **kwargs):
        from google.cloud.firestore_v1.batch import MutationsBatcher

        if table is None:
            table = mock.Mock()

        return MutationsBatcher(table, **kwargs)

    def test_ctor(self):
        pass

    def test_context_manager(self):
        pass

    def test__flush_timer(self):
        pass

    def test_close(self):
        pass

    def test_append(self):
        pass

    def test_flush(self):
        pass

    def test__raise_exceptions(self):
        pass

