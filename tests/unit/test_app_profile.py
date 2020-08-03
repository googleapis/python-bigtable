# Copyright 2018 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest

import mock

from ._testing import _make_credentials

from tests.unit.test_base_app_profile import (
    ChannelStub,
    TestAppProfileConstants,
    _Client,
    _Instance,
)


class TestAppProfile(unittest.TestCase, TestAppProfileConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.app_profile import AppProfile

        return AppProfile

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor_defaults(self):
        client = _Client(self.PROJECT)
        instance = _Instance(self.INSTANCE_ID, client)

        app_profile = self._make_one(self.APP_PROFILE_ID, instance)
        self.assertIsInstance(app_profile, self._get_target_class())
        self.assertEqual(app_profile._instance, instance)
        self.assertIsNone(app_profile.routing_policy_type)
        self.assertIsNone(app_profile.description)
        self.assertIsNone(app_profile.cluster_id)
        self.assertIsNone(app_profile.allow_transactional_writes)

    def test_reload_routing_any(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable.enums import RoutingPolicyType

        api = bigtable_instance_admin_client.BigtableInstanceAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = _Instance(self.INSTANCE_ID, client)

        routing = RoutingPolicyType.ANY
        description = "routing policy any"

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
        )

        # Create response_pb
        description_from_server = "routing policy switched to single"
        cluster_id_from_server = self.CLUSTER_ID
        allow_transactional_writes = True
        single_cluster_routing = data_v2_pb2.AppProfile.SingleClusterRouting(
            cluster_id=cluster_id_from_server,
            allow_transactional_writes=allow_transactional_writes,
        )

        response_pb = data_v2_pb2.AppProfile(
            name=app_profile.name,
            single_cluster_routing=single_cluster_routing,
            description=description_from_server,
        )

        # Patch the stub used by the API method.
        client._instance_admin_client = api
        instance_stub = client._instance_admin_client.transport
        instance_stub.get_app_profile.side_effect = [response_pb]

        # Create expected_result.
        expected_result = None  # reload() has no return value.

        # Check app_profile config values before.
        self.assertEqual(app_profile.routing_policy_type, routing)
        self.assertEqual(app_profile.description, description)
        self.assertIsNone(app_profile.cluster_id)
        self.assertIsNone(app_profile.allow_transactional_writes)

        # Perform the method and check the result.
        result = app_profile.reload()
        self.assertEqual(result, expected_result)
        self.assertEqual(app_profile.routing_policy_type, RoutingPolicyType.SINGLE)
        self.assertEqual(app_profile.description, description_from_server)
        self.assertEqual(app_profile.cluster_id, cluster_id_from_server)
        self.assertEqual(
            app_profile.allow_transactional_writes, allow_transactional_writes
        )

    def test_exists(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.api_core import exceptions

        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            mock.Mock()
        )
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)

        # Create response_pb
        response_pb = data_v2_pb2.AppProfile(name=self.APP_PROFILE_NAME)
        client._instance_admin_client = instance_api

        # Patch the stub used by the API method.
        client._instance_admin_client = instance_api
        instance_stub = client._instance_admin_client.transport
        instance_stub.get_app_profile.side_effect = [
            response_pb,
            exceptions.NotFound("testing"),
            exceptions.BadRequest("testing"),
        ]

        # Perform the method and check the result.
        non_existing_app_profile_id = "other-app-profile-id"
        app_profile = self._make_one(self.APP_PROFILE_ID, instance)
        alt_app_profile = self._make_one(non_existing_app_profile_id, instance)
        self.assertTrue(app_profile.exists())
        self.assertFalse(alt_app_profile.exists())
        with self.assertRaises(exceptions.BadRequest):
            alt_app_profile.exists()

    def test_create_routing_any(self):
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client

        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)

        routing = RoutingPolicyType.ANY
        description = "routing policy any"
        ignore_warnings = True

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
        )
        expected_request_app_profile = app_profile._to_pb()
        expected_request = messages_v2_pb2.CreateAppProfileRequest(
            parent=instance.name,
            app_profile_id=self.APP_PROFILE_ID,
            app_profile=expected_request_app_profile,
            ignore_warnings=ignore_warnings,
        )

        # Patch the stub used by the API method.
        channel = ChannelStub(responses=[expected_request_app_profile])
        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            channel=channel
        )
        client._instance_admin_client = instance_api
        # Perform the method and check the result.
        result = app_profile.create(ignore_warnings)
        actual_request = channel.requests[0][1]

        self.assertEqual(actual_request, expected_request)
        self.assertIsInstance(result, self._get_target_class())
        self.assertEqual(result.app_profile_id, self.APP_PROFILE_ID)
        self.assertIs(result._instance, instance)
        self.assertEqual(result.routing_policy_type, routing)
        self.assertEqual(result.description, description)
        self.assertEqual(result.allow_transactional_writes, False)
        self.assertIsNone(result.cluster_id)

    def test_create_routing_single(self):
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client

        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)

        routing = RoutingPolicyType.SINGLE
        description = "routing policy single"
        allow_writes = False
        ignore_warnings = True

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
            cluster_id=self.CLUSTER_ID,
            allow_transactional_writes=allow_writes,
        )
        expected_request_app_profile = app_profile._to_pb()
        expected_request = messages_v2_pb2.CreateAppProfileRequest(
            parent=instance.name,
            app_profile_id=self.APP_PROFILE_ID,
            app_profile=expected_request_app_profile,
            ignore_warnings=ignore_warnings,
        )

        # Patch the stub used by the API method.
        channel = ChannelStub(responses=[expected_request_app_profile])
        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            channel=channel
        )
        client._instance_admin_client = instance_api
        # Perform the method and check the result.
        result = app_profile.create(ignore_warnings)
        actual_request = channel.requests[0][1]

        self.assertEqual(actual_request, expected_request)
        self.assertIsInstance(result, self._get_target_class())
        self.assertEqual(result.app_profile_id, self.APP_PROFILE_ID)
        self.assertIs(result._instance, instance)
        self.assertEqual(result.routing_policy_type, routing)
        self.assertEqual(result.description, description)
        self.assertEqual(result.allow_transactional_writes, allow_writes)
        self.assertEqual(result.cluster_id, self.CLUSTER_ID)

    def test_create_app_profile_with_wrong_routing_policy(self):
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)
        app_profile = self._make_one(
            self.APP_PROFILE_ID, instance, routing_policy_type=None
        )
        with self.assertRaises(ValueError):
            app_profile.create()

    def test_update_app_profile_routing_any(self):
        from google.api_core import operation
        from google.longrunning import operations_pb2
        from google.protobuf.any_pb2 import Any
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.protobuf import field_mask_pb2

        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)

        routing = RoutingPolicyType.SINGLE
        description = "to routing policy single"
        allow_writes = True
        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
            cluster_id=self.CLUSTER_ID,
            allow_transactional_writes=allow_writes,
        )

        # Create response_pb
        metadata = messages_v2_pb2.UpdateAppProfileMetadata()
        type_url = "type.googleapis.com/{}".format(
            messages_v2_pb2.UpdateAppProfileMetadata.DESCRIPTOR.full_name
        )
        response_pb = operations_pb2.Operation(
            name=self.OP_NAME,
            metadata=Any(type_url=type_url, value=metadata.SerializeToString()),
        )

        # Patch the stub used by the API method.
        channel = ChannelStub(responses=[response_pb])
        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            channel=channel
        )
        # Mock api calls
        client._instance_admin_client = instance_api

        # Perform the method and check the result.
        ignore_warnings = True
        expected_request_update_mask = field_mask_pb2.FieldMask(
            paths=["description", "single_cluster_routing"]
        )
        expected_request = messages_v2_pb2.UpdateAppProfileRequest(
            app_profile=app_profile._to_pb(),
            update_mask=expected_request_update_mask,
            ignore_warnings=ignore_warnings,
        )

        result = app_profile.update(ignore_warnings=ignore_warnings)
        actual_request = channel.requests[0][1]

        self.assertEqual(actual_request, expected_request)
        self.assertIsInstance(result, operation.Operation)
        self.assertEqual(result.operation.name, self.OP_NAME)
        self.assertIsInstance(result.metadata, messages_v2_pb2.UpdateAppProfileMetadata)

    def test_update_app_profile_routing_single(self):
        from google.api_core import operation
        from google.longrunning import operations_pb2
        from google.protobuf.any_pb2 import Any
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.protobuf import field_mask_pb2

        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)

        routing = RoutingPolicyType.ANY
        app_profile = self._make_one(
            self.APP_PROFILE_ID, instance, routing_policy_type=routing
        )

        # Create response_pb
        metadata = messages_v2_pb2.UpdateAppProfileMetadata()
        type_url = "type.googleapis.com/{}".format(
            messages_v2_pb2.UpdateAppProfileMetadata.DESCRIPTOR.full_name
        )
        response_pb = operations_pb2.Operation(
            name=self.OP_NAME,
            metadata=Any(type_url=type_url, value=metadata.SerializeToString()),
        )

        # Patch the stub used by the API method.
        channel = ChannelStub(responses=[response_pb])
        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            channel=channel
        )
        # Mock api calls
        client._instance_admin_client = instance_api

        # Perform the method and check the result.
        ignore_warnings = True
        expected_request_update_mask = field_mask_pb2.FieldMask(
            paths=["multi_cluster_routing_use_any"]
        )
        expected_request = messages_v2_pb2.UpdateAppProfileRequest(
            app_profile=app_profile._to_pb(),
            update_mask=expected_request_update_mask,
            ignore_warnings=ignore_warnings,
        )

        result = app_profile.update(ignore_warnings=ignore_warnings)
        actual_request = channel.requests[0][1]

        self.assertEqual(actual_request, expected_request)
        self.assertIsInstance(result, operation.Operation)
        self.assertEqual(result.operation.name, self.OP_NAME)
        self.assertIsInstance(result.metadata, messages_v2_pb2.UpdateAppProfileMetadata)

    def test_update_app_profile_with_wrong_routing_policy(self):
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)
        app_profile = self._make_one(
            self.APP_PROFILE_ID, instance, routing_policy_type=None
        )
        with self.assertRaises(ValueError):
            app_profile.update()

    def test_delete(self):
        from google.protobuf import empty_pb2
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client

        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            mock.Mock()
        )

        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = client.instance(self.INSTANCE_ID)
        app_profile = self._make_one(self.APP_PROFILE_ID, instance)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Patch the stub used by the API method.
        client._instance_admin_client = instance_api
        instance_stub = client._instance_admin_client.transport
        instance_stub.delete_cluster.side_effect = [response_pb]

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # Perform the method and check the result.
        result = app_profile.delete()

        self.assertEqual(result, expected_result)
