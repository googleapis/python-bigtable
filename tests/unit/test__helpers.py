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

import pytest
import google.cloud.bigtable._helpers as _helpers


class Test_MakeMetadata:
    @pytest.mark.parametrize(
        "table,profile,expected",
        [
            ("table", "profile", "table_name=table,app_profile_id=profile"),
            (None, "profile", "app_profile_id=profile"),
            ("table", None, "table_name=table"),
            (None, None, ""),
        ],
    )
    def test__make_metadata(self, table, profile, expected):
        metadata = _helpers._make_metadata(table, profile)
        assert metadata == [("x-goog-request-params", expected)]
