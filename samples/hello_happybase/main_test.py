# Copyright 2016 Google Inc.
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

import os

from .main import main
from google.cloud import bigtable

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BIGTABLE_INSTANCE = os.environ["BIGTABLE_INSTANCE"]
TABLE_ID = "hello-world-hb-test"


def test_main(capsys):
    try:
        main(PROJECT, BIGTABLE_INSTANCE, TABLE_ID)

        out, _ = capsys.readouterr()
        assert "Creating the {} table.".format(TABLE_ID) in out
        assert "Writing some greetings to the table." in out
        assert "Getting a single greeting by row key." in out
        assert "Hello World!" in out
        assert "Scanning for all greetings" in out
        assert "Hello Cloud Bigtable!" in out
        assert "Deleting the {} table.".format(TABLE_ID) in out
    finally:
        # delete table
        client = bigtable.Client(PROJECT, admin=True)
        instance = client.instance(BIGTABLE_INSTANCE)
        table = instance.table(TABLE_ID)
        if table.exists():
            table.delete()
