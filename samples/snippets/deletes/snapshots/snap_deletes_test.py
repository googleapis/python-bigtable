# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot


snapshots = Snapshot()

snapshots['test_check_and_mutate 1'] = '''Successfully deleted row cells.
Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190405.004 @2019-05-01 00:00:00+00:00

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190401.002 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

<snapshottest.pytest.PyTestSnapshotTest object at 0x2919cf460>
'''

snapshots['test_delete_column_family 1'] = '''Successfully deleted column family stats_summary.
Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00

'''

snapshots['test_delete_from_column 1'] = '''Successfully deleted column 'data_plan_01gb' from column family 'cell_plan'.
Reading data for phone#4c410523#20190501:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190405.003 @2019-05-01 00:00:00+00:00

Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190405.004 @2019-05-01 00:00:00+00:00

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190401.002 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

<snapshottest.pytest.PyTestSnapshotTest object at 0x2919bad30>
'''

snapshots['test_delete_from_column_family 1'] = '''Successfully deleted columns 'data_plan_01gb' and 'data_plan_05gb' from column family 'cell_plan'.
Reading data for phone#4c410523#20190501:
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190405.003 @2019-05-01 00:00:00+00:00

Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190405.004 @2019-05-01 00:00:00+00:00

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190401.002 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

<snapshottest.pytest.PyTestSnapshotTest object at 0x2919badf0>
'''

snapshots['test_delete_from_row 1'] = '''Successfully deleted row b'phone#4c410523#20190501'
Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190405.004 @2019-05-01 00:00:00+00:00

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190401.002 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

'''

snapshots['test_delete_table 1'] = '''Successfully deleted table mobile-time-series-94a0c0b5-67ae-47
'''

snapshots['test_drop_row_range 1'] = '''Successfully deleted rows with prefix Pb'phone#4c'
Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190401.002 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

'''

snapshots['test_streaming_and_batching 1'] = '''Successfully deleted rows in batches of 2
Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190405.004 @2019-05-01 00:00:00+00:00

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190401.002 @2019-05-01 00:00:00+00:00

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true @2019-05-01 00:00:00+00:00
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01 @2019-05-01 00:00:00+00:00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00 @2019-05-01 00:00:00+00:00
\tos_build: PQ2A.190406.000 @2019-05-01 00:00:00+00:00

'''
