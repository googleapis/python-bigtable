#!/usr/bin/env python

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

"""Demonstrates how to connect to Cloud Bigtable and run some basic operations.

Prerequisites:

- Create a Cloud Bigtable cluster.
  https://cloud.google.com/bigtable/docs/creating-cluster
- Set your Google Application Default Credentials.
  https://developers.google.com/identity/protocols/application-default-credentials
"""

import argparse

# [START bigtable_hw_imports_happybase]
from google.cloud import bigtable
from google.cloud import happybase
# [END bigtable_hw_imports_happybase]


def main(project_id, instance_id, table_name):
    # [START bigtable_hw_connect_happybase]
    # The client must be created with admin=True because it will create a
    # table.
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)
    # [END bigtable_hw_connect_happybase]

    try:
        # [START bigtable_hw_create_table_happybase]
        print('Creating the {} table.'.format(table_name))
        column_family_name = 'cf1'
        connection.create_table(
            table_name,
            {
                column_family_name: dict()  # Use default options.
            })
        # [END bigtable_hw_create_table_happybase]

        # [START bigtable_hw_write_rows_happybase]
        print('Writing some greetings to the table.')
        table = connection.table(table_name)
        column_name = '{fam}:greeting'.format(fam=column_family_name)
        greetings = [
            'Hello World!',
            'Hello Cloud Bigtable!',
            'Hello HappyBase!',
        ]

        for i, value in enumerate(greetings):
            # Note: This example uses sequential numeric IDs for simplicity,
            # but this can result in poor performance in a production
            # application.  Since rows are stored in sorted order by key,
            # sequential keys can result in poor distribution of operations
            # across nodes.
            #
            # For more information about how to design a Bigtable schema for
            # the best performance, see the documentation:
            #
            #     https://cloud.google.com/bigtable/docs/schema-design
            row_key = 'greeting{}'.format(i)
            table.put(
                row_key, {column_name.encode('utf-8'): value.encode('utf-8')}
            )
        # [END bigtable_hw_write_rows_happybase]

        # [START bigtable_hw_get_by_key_happybase]
        print('Getting a single greeting by row key.')
        key = 'greeting0'.encode('utf-8')
        row = table.row(key)
        print('\t{}: {}'.format(key, row[column_name.encode('utf-8')]))
        # [END bigtable_hw_get_by_key_happybase]

        # [START bigtable_hw_scan_all_happybase]
        print('Scanning for all greetings:')

        for key, row in table.scan():
            print('\t{}: {}'.format(key, row[column_name.encode('utf-8')]))
        # [END bigtable_hw_scan_all_happybase]

        # [START bigtable_hw_delete_table_happybase]
        print('Deleting the {} table.'.format(table_name))
        connection.delete_table(table_name)
        # [END bigtable_hw_delete_table_happybase]

    finally:
        connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('project_id', help='Your Cloud Platform project ID.')
    parser.add_argument(
        'instance_id', help='ID of the Cloud Bigtable instance to connect to.')
    parser.add_argument(
        '--table',
        help='Table to create and destroy.',
        default='Hello-Bigtable')

    args = parser.parse_args()
    main(args.project_id, args.instance_id, args.table)
