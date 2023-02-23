# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: test_proxy.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.api import client_pb2 as google_dot_api_dot_client__pb2
import bigtable_pb2 as google_dot_bigtable_dot_v2_dot_bigtable__pb2
import data_pb2 as google_dot_bigtable_dot_v2_dot_data__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.rpc import status_pb2 as google_dot_rpc_dot_status__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10test_proxy.proto\x12\x19google.bigtable.testproxy\x1a\x17google/api/client.proto\x1a!google/bigtable/v2/bigtable.proto\x1a\x1dgoogle/bigtable/v2/data.proto\x1a\x1egoogle/protobuf/duration.proto\x1a\x17google/rpc/status.proto\"\xb8\x01\n\x13\x43reateClientRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x13\n\x0b\x64\x61ta_target\x18\x02 \x01(\t\x12\x12\n\nproject_id\x18\x03 \x01(\t\x12\x13\n\x0binstance_id\x18\x04 \x01(\t\x12\x16\n\x0e\x61pp_profile_id\x18\x05 \x01(\t\x12\x38\n\x15per_operation_timeout\x18\x06 \x01(\x0b\x32\x19.google.protobuf.Duration\"\x16\n\x14\x43reateClientResponse\"\'\n\x12\x43loseClientRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\"\x15\n\x13\x43loseClientResponse\"(\n\x13RemoveClientRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\"\x16\n\x14RemoveClientResponse\"w\n\x0eReadRowRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x12\n\ntable_name\x18\x04 \x01(\t\x12\x0f\n\x07row_key\x18\x02 \x01(\t\x12-\n\x06\x66ilter\x18\x03 \x01(\x0b\x32\x1d.google.bigtable.v2.RowFilter\"U\n\tRowResult\x12\"\n\x06status\x18\x01 \x01(\x0b\x32\x12.google.rpc.Status\x12$\n\x03row\x18\x02 \x01(\x0b\x32\x17.google.bigtable.v2.Row\"u\n\x0fReadRowsRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x34\n\x07request\x18\x02 \x01(\x0b\x32#.google.bigtable.v2.ReadRowsRequest\x12\x19\n\x11\x63\x61ncel_after_rows\x18\x03 \x01(\x05\"V\n\nRowsResult\x12\"\n\x06status\x18\x01 \x01(\x0b\x32\x12.google.rpc.Status\x12$\n\x03row\x18\x02 \x03(\x0b\x32\x17.google.bigtable.v2.Row\"\\\n\x10MutateRowRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x35\n\x07request\x18\x02 \x01(\x0b\x32$.google.bigtable.v2.MutateRowRequest\"5\n\x0fMutateRowResult\x12\"\n\x06status\x18\x01 \x01(\x0b\x32\x12.google.rpc.Status\"^\n\x11MutateRowsRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x36\n\x07request\x18\x02 \x01(\x0b\x32%.google.bigtable.v2.MutateRowsRequest\"s\n\x10MutateRowsResult\x12\"\n\x06status\x18\x01 \x01(\x0b\x32\x12.google.rpc.Status\x12;\n\x05\x65ntry\x18\x02 \x03(\x0b\x32,.google.bigtable.v2.MutateRowsResponse.Entry\"l\n\x18\x43heckAndMutateRowRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12=\n\x07request\x18\x02 \x01(\x0b\x32,.google.bigtable.v2.CheckAndMutateRowRequest\"|\n\x17\x43heckAndMutateRowResult\x12\"\n\x06status\x18\x01 \x01(\x0b\x32\x12.google.rpc.Status\x12=\n\x06result\x18\x02 \x01(\x0b\x32-.google.bigtable.v2.CheckAndMutateRowResponse\"d\n\x14SampleRowKeysRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x39\n\x07request\x18\x02 \x01(\x0b\x32(.google.bigtable.v2.SampleRowKeysRequest\"t\n\x13SampleRowKeysResult\x12\"\n\x06status\x18\x01 \x01(\x0b\x32\x12.google.rpc.Status\x12\x39\n\x06sample\x18\x02 \x03(\x0b\x32).google.bigtable.v2.SampleRowKeysResponse\"n\n\x19ReadModifyWriteRowRequest\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12>\n\x07request\x18\x02 \x01(\x0b\x32-.google.bigtable.v2.ReadModifyWriteRowRequest2\xa4\t\n\x18\x43loudBigtableV2TestProxy\x12q\n\x0c\x43reateClient\x12..google.bigtable.testproxy.CreateClientRequest\x1a/.google.bigtable.testproxy.CreateClientResponse\"\x00\x12n\n\x0b\x43loseClient\x12-.google.bigtable.testproxy.CloseClientRequest\x1a..google.bigtable.testproxy.CloseClientResponse\"\x00\x12q\n\x0cRemoveClient\x12..google.bigtable.testproxy.RemoveClientRequest\x1a/.google.bigtable.testproxy.RemoveClientResponse\"\x00\x12\\\n\x07ReadRow\x12).google.bigtable.testproxy.ReadRowRequest\x1a$.google.bigtable.testproxy.RowResult\"\x00\x12_\n\x08ReadRows\x12*.google.bigtable.testproxy.ReadRowsRequest\x1a%.google.bigtable.testproxy.RowsResult\"\x00\x12\x66\n\tMutateRow\x12+.google.bigtable.testproxy.MutateRowRequest\x1a*.google.bigtable.testproxy.MutateRowResult\"\x00\x12m\n\x0e\x42ulkMutateRows\x12,.google.bigtable.testproxy.MutateRowsRequest\x1a+.google.bigtable.testproxy.MutateRowsResult\"\x00\x12~\n\x11\x43heckAndMutateRow\x12\x33.google.bigtable.testproxy.CheckAndMutateRowRequest\x1a\x32.google.bigtable.testproxy.CheckAndMutateRowResult\"\x00\x12r\n\rSampleRowKeys\x12/.google.bigtable.testproxy.SampleRowKeysRequest\x1a..google.bigtable.testproxy.SampleRowKeysResult\"\x00\x12r\n\x12ReadModifyWriteRow\x12\x34.google.bigtable.testproxy.ReadModifyWriteRowRequest\x1a$.google.bigtable.testproxy.RowResult\"\x00\x1a\x34\xca\x41\x31\x62igtable-test-proxy-not-accessible.googleapis.comB6\n#com.google.cloud.bigtable.testproxyP\x01Z\r./testproxypbb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'test_proxy_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n#com.google.cloud.bigtable.testproxyP\001Z\r./testproxypb'
  _CLOUDBIGTABLEV2TESTPROXY._options = None
  _CLOUDBIGTABLEV2TESTPROXY._serialized_options = b'\312A1bigtable-test-proxy-not-accessible.googleapis.com'
  _CREATECLIENTREQUEST._serialized_start=196
  _CREATECLIENTREQUEST._serialized_end=380
  _CREATECLIENTRESPONSE._serialized_start=382
  _CREATECLIENTRESPONSE._serialized_end=404
  _CLOSECLIENTREQUEST._serialized_start=406
  _CLOSECLIENTREQUEST._serialized_end=445
  _CLOSECLIENTRESPONSE._serialized_start=447
  _CLOSECLIENTRESPONSE._serialized_end=468
  _REMOVECLIENTREQUEST._serialized_start=470
  _REMOVECLIENTREQUEST._serialized_end=510
  _REMOVECLIENTRESPONSE._serialized_start=512
  _REMOVECLIENTRESPONSE._serialized_end=534
  _READROWREQUEST._serialized_start=536
  _READROWREQUEST._serialized_end=655
  _ROWRESULT._serialized_start=657
  _ROWRESULT._serialized_end=742
  _READROWSREQUEST._serialized_start=744
  _READROWSREQUEST._serialized_end=861
  _ROWSRESULT._serialized_start=863
  _ROWSRESULT._serialized_end=949
  _MUTATEROWREQUEST._serialized_start=951
  _MUTATEROWREQUEST._serialized_end=1043
  _MUTATEROWRESULT._serialized_start=1045
  _MUTATEROWRESULT._serialized_end=1098
  _MUTATEROWSREQUEST._serialized_start=1100
  _MUTATEROWSREQUEST._serialized_end=1194
  _MUTATEROWSRESULT._serialized_start=1196
  _MUTATEROWSRESULT._serialized_end=1311
  _CHECKANDMUTATEROWREQUEST._serialized_start=1313
  _CHECKANDMUTATEROWREQUEST._serialized_end=1421
  _CHECKANDMUTATEROWRESULT._serialized_start=1423
  _CHECKANDMUTATEROWRESULT._serialized_end=1547
  _SAMPLEROWKEYSREQUEST._serialized_start=1549
  _SAMPLEROWKEYSREQUEST._serialized_end=1649
  _SAMPLEROWKEYSRESULT._serialized_start=1651
  _SAMPLEROWKEYSRESULT._serialized_end=1767
  _READMODIFYWRITEROWREQUEST._serialized_start=1769
  _READMODIFYWRITEROWREQUEST._serialized_end=1879
  _CLOUDBIGTABLEV2TESTPROXY._serialized_start=1882
  _CLOUDBIGTABLEV2TESTPROXY._serialized_end=3070
# @@protoc_insertion_point(module_scope)
