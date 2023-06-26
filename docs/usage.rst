Using the API
=============

.. toctree::
  :maxdepth: 2

  client-intro
  client
  cluster
  instance
  table
  app-profile
  backup
  column-family
  encryption-info
  row
  row-data
  row-filters
  row-set
  batcher


In the hierarchy of API concepts

* a :class:`Client <google.cloud.bigtable.deprecated.client.Client>` owns an
  :class:`Instance <google.cloud.bigtable.deprecated.instance.Instance>`
* an :class:`Instance <google.cloud.bigtable.deprecated.instance.Instance>` owns a
  :class:`Table <google.cloud.bigtable.deprecated.table.Table>`
* a :class:`Table <google.cloud.bigtable.deprecated.table.Table>` owns a
  :class:`ColumnFamily <google.cloud.bigtable.deprecated.column_family.ColumnFamily>`
* a :class:`Table <google.cloud.bigtable.deprecated.table.Table>` owns a
  :class:`Row <google.cloud.bigtable.deprecated.row.Row>`
  (and all the cells in the row)
