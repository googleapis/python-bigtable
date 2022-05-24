# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot


snapshots = Snapshot()

snapshots['test_check_and_mutate 1'] = '''Successfully deleted row cells.
Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190405.004

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190406.000

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190401.002

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00
\tos_build: PQ2A.190406.000

'''

snapshots['test_delete_column_family 1'] = '''Successfully deleted column family stats_summary.
Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true

'''

snapshots['test_delete_from_column 1'] = '''Successfully deleted column 'data_plan_01gb' from column family 'cell_plan'.
Reading data for phone#4c410523#20190501:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190405.003

Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190405.004

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190406.000

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190401.002

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00
\tos_build: PQ2A.190406.000

'''

snapshots['test_delete_from_column_family 1'] = '''Successfully deleted columns 'data_plan_01gb' and 'data_plan_05gb' from column family 'cell_plan'.
Reading data for phone#4c410523#20190501:
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190405.003

Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190405.004

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190406.000

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190401.002

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00
\tos_build: PQ2A.190406.000

'''

snapshots['test_delete_from_row 1'] = '''Successfully deleted row b'phone#4c410523#20190501'
Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190405.004

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190406.000

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190401.002

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00
\tos_build: PQ2A.190406.000

'''

snapshots['test_delete_table 1'] = '''Successfully deleted table.
'''

snapshots['test_drop_row_range 1'] = '''Successfully deleted rows with prefix Pb'phone#4c'
Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190401.002

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00
\tos_build: PQ2A.190406.000

'''

snapshots['test_streaming_and_batching 1'] = '''Successfully deleted rows in batches of 2
Reading data for phone#4c410523#20190502:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190405.004

Reading data for phone#4c410523#20190505:
Column Family cell_plan
\tdata_plan_05gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x00
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190406.000

Reading data for phone#5c10102#20190501:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x01
\tos_build: PQ2A.190401.002

Reading data for phone#5c10102#20190502:
Column Family cell_plan
\tdata_plan_10gb: true
Column Family stats_summary
\tconnected_cell: \x00\x00\x00\x00\x00\x00\x00\x01
\tconnected_wifi: \x00\x00\x00\x00\x00\x00\x00\x00
\tos_build: PQ2A.190406.000

'''
