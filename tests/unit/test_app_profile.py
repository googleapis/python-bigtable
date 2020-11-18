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


class TestAppProfile(unittest.TestCase):

    PROJECT = "project"
    INSTANCE_ID = "instance-id"
    APP_PROFILE_ID = "app-profile-id"
    APP_PROFILE_NAME = "projects/{}/instances/{}/appProfiles/{}".format(
        PROJECT, INSTANCE_ID, APP_PROFILE_ID
    )
    CLUSTER_ID = "cluster-id"

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.app_profile import AppProfile

        return AppProfile

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def _mock_instance(self):
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable.instance import Instance
        from google.cloud.bigtable_admin_v2.gapic import (
            bigtable_instance_admin_client as admin_client_module,
        )

        admin_api = mock.create_autospec(
            admin_client_module.BigtableInstanceAdminClient,
            instance=True,
        )
        admin_api.app_profile_path.return_value = self.APP_PROFILE_NAME

        client = mock.create_autospec(Client, instance=True, project=self.PROJECT)
        client.instance_admin_client = admin_api

        instance_name = "projects/{}/instances/{}".format(
            self.PROJECT,
            self.INSTANCE_ID,
        )
        instance = mock.create_autospec(
            Instance,
            instance=True,
            _client=client,
            instance_id=self.INSTANCE_ID,
        )
        instance.name = instance_name

        return admin_api, client, instance

    def test_constructor_defaults(self):
        _, client, instance = self._mock_instance()

        app_profile = self._make_one(self.APP_PROFILE_ID, instance)

        self.assertIsInstance(app_profile, self._get_target_class())
        self.assertIs(app_profile._instance, instance)
        self.assertIsNone(app_profile.routing_policy_type)
        self.assertIsNone(app_profile.description)
        self.assertIsNone(app_profile.cluster_id)
        self.assertIsNone(app_profile.allow_transactional_writes)

    def test_constructor_w_routing_any(self):
        from google.cloud.bigtable.enums import RoutingPolicyType

        _, _, instance = self._mock_instance()

        description = "routing policy any"

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=RoutingPolicyType.ANY,
            description=description,
        )

        self.assertEqual(app_profile.app_profile_id, self.APP_PROFILE_ID)
        self.assertIs(app_profile._instance, instance)
        self.assertEqual(app_profile.routing_policy_type, RoutingPolicyType.ANY)
        self.assertEqual(app_profile.description, description)
        self.assertIsNone(app_profile.cluster_id)
        self.assertIsNone(app_profile.allow_transactional_writes)

    def test_constructor_w_routing_single(self):
        from google.cloud.bigtable.enums import RoutingPolicyType

        _, _, instance = self._mock_instance()
        description = "routing policy single"

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=RoutingPolicyType.SINGLE,
            description=description,
            cluster_id=self.CLUSTER_ID,
            allow_transactional_writes=True,
        )

        self.assertEqual(app_profile.app_profile_id, self.APP_PROFILE_ID)
        self.assertIs(app_profile._instance, instance)
        self.assertEqual(app_profile.routing_policy_type, RoutingPolicyType.SINGLE)
        self.assertEqual(app_profile.description, description)
        self.assertEqual(app_profile.cluster_id, self.CLUSTER_ID)
        self.assertEqual(app_profile.allow_transactional_writes, True)

    def test_name_property(self):
        _, _, instance = self._mock_instance()

        app_profile = self._make_one(self.APP_PROFILE_ID, instance)
        self.assertEqual(app_profile.name, self.APP_PROFILE_NAME)

    def test___eq__(self):
        _, _, instance = self._mock_instance()

        app_profile1 = self._make_one(self.APP_PROFILE_ID, instance)
        app_profile2 = self._make_one(self.APP_PROFILE_ID, instance)

        self.assertTrue(app_profile1 == app_profile2)

    def test___eq__other_type(self):
        _, _, instance = self._mock_instance()

        other_object = object()
        app_profile = self._make_one(self.APP_PROFILE_ID, instance)
        self.assertFalse(app_profile == other_object)

    def test___eq__other_instance(self):
        _, _, instance = self._mock_instance()

        alt_instance = mock.Mock()
        app_profile1 = self._make_one(self.APP_PROFILE_ID, instance)
        app_profile2 = self._make_one(self.APP_PROFILE_ID, alt_instance)
        self.assertFalse(app_profile1 == app_profile2)

    def test___ne__same_value(self):
        _, _, instance = self._mock_instance()
        app_profile1 = self._make_one(self.APP_PROFILE_ID, instance)
        app_profile2 = self._make_one(self.APP_PROFILE_ID, instance)
        self.assertFalse(app_profile1 != app_profile2)

    def test___ne__(self):
        _, _, instance = self._mock_instance()
        app_profile1 = self._make_one("app_profile_id1", instance)
        app_profile2 = self._make_one("app_profile_id2", instance)
        self.assertTrue(app_profile1 != app_profile2)

    def test_from_pb_success_routing_any(self):
        from google.cloud.bigtable_admin_v2.types import instance_pb2
        from google.cloud.bigtable.enums import RoutingPolicyType

        _, _, instance = self._mock_instance()

        description = "routing any"
        routing = RoutingPolicyType.ANY
        multi_cluster_routing_use_any = (
            instance_pb2.AppProfile.MultiClusterRoutingUseAny()
        )

        app_profile_pb = instance_pb2.AppProfile(
            name=self.APP_PROFILE_NAME,
            description=description,
            multi_cluster_routing_use_any=multi_cluster_routing_use_any,
        )

        klass = self._get_target_class()
        app_profile = klass.from_pb(app_profile_pb, instance)
        self.assertIsInstance(app_profile, klass)
        self.assertIs(app_profile._instance, instance)
        self.assertEqual(app_profile.app_profile_id, self.APP_PROFILE_ID)
        self.assertEqual(app_profile.description, description)
        self.assertEqual(app_profile.routing_policy_type, routing)
        self.assertIsNone(app_profile.cluster_id)
        self.assertEqual(app_profile.allow_transactional_writes, False)

    def test_from_pb_success_routing_single(self):
        from google.cloud.bigtable_admin_v2.types import instance_pb2
        from google.cloud.bigtable.enums import RoutingPolicyType

        _, _, instance = self._mock_instance()

        description = "routing single"
        allow_transactional_writes = True
        routing = RoutingPolicyType.SINGLE
        single_cluster_routing = instance_pb2.AppProfile.SingleClusterRouting(
            cluster_id=self.CLUSTER_ID,
            allow_transactional_writes=allow_transactional_writes,
        )

        app_profile_pb = instance_pb2.AppProfile(
            name=self.APP_PROFILE_NAME,
            description=description,
            single_cluster_routing=single_cluster_routing,
        )

        klass = self._get_target_class()
        app_profile = klass.from_pb(app_profile_pb, instance)
        self.assertIsInstance(app_profile, klass)
        self.assertIs(app_profile._instance, instance)
        self.assertEqual(app_profile.app_profile_id, self.APP_PROFILE_ID)
        self.assertEqual(app_profile.description, description)
        self.assertEqual(app_profile.routing_policy_type, routing)
        self.assertEqual(app_profile.cluster_id, self.CLUSTER_ID)
        self.assertEqual(
            app_profile.allow_transactional_writes, allow_transactional_writes
        )

    def test_from_pb_bad_app_profile_name(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        _, _, instance = self._mock_instance()
        bad_app_profile_name = "BAD_NAME"
        app_profile_pb = instance_pb2.AppProfile(name=bad_app_profile_name)

        klass = self._get_target_class()
        with self.assertRaises(ValueError):
            klass.from_pb(app_profile_pb, instance)

    def test_from_pb_instance_id_mistmatch(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        _, _, instance = self._mock_instance()
        ALT_INSTANCE_ID = "ALT_INSTANCE_ID"
        instance.instance_id = ALT_INSTANCE_ID

        app_profile_pb = instance_pb2.AppProfile(name=self.APP_PROFILE_NAME)

        klass = self._get_target_class()
        with self.assertRaises(ValueError):
            klass.from_pb(app_profile_pb, instance)

    def test_from_pb_project_mistmatch(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        _, client, instance = self._mock_instance()
        client.project = "ALT_PROJECT"

        app_profile_pb = instance_pb2.AppProfile(name=self.APP_PROFILE_NAME)

        klass = self._get_target_class()
        with self.assertRaises(ValueError):
            klass.from_pb(app_profile_pb, instance)

    def test__update_from_pb_w_routing_any(self):
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        _, _, instance = self._mock_instance()
        other_profile_id = "other-id"
        description = "testing"

        app_profile = self._make_one(other_profile_id, instance)
        multi_cluster_routing_use_any = (
            instance_pb2.AppProfile.MultiClusterRoutingUseAny()
        )
        app_profile_pb = instance_pb2.AppProfile(
            name=self.APP_PROFILE_NAME,
            multi_cluster_routing_use_any=multi_cluster_routing_use_any,
            description=description,
        )

        app_profile._update_from_pb(app_profile_pb)

        self.assertEqual(app_profile.app_profile_id, other_profile_id)
        self.assertEqual(app_profile.description, description)
        self.assertEqual(app_profile.routing_policy_type, RoutingPolicyType.ANY)
        self.assertIsNone(app_profile.cluster_id)
        self.assertFalse(app_profile.allow_transactional_writes)

    def test__update_from_pb_w_routing_single(self):
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        _, _, instance = self._mock_instance()
        other_profile_id = "other-id"
        description = "testing"

        app_profile = self._make_one(other_profile_id, instance)

        single_cluster_routing = instance_pb2.AppProfile.SingleClusterRouting(
            cluster_id=self.CLUSTER_ID,
            allow_transactional_writes=True,
        )
        app_profile_pb = instance_pb2.AppProfile(
            name=self.APP_PROFILE_NAME,
            description=description,
            single_cluster_routing=single_cluster_routing,
        )

        app_profile._update_from_pb(app_profile_pb)

        self.assertEqual(app_profile.app_profile_id, other_profile_id)
        self.assertEqual(app_profile.description, description)
        self.assertEqual(app_profile.routing_policy_type, RoutingPolicyType.SINGLE)
        self.assertEqual(app_profile.cluster_id, self.CLUSTER_ID)
        self.assertTrue(app_profile.allow_transactional_writes)

    def test__to_pb_routing_any(self):
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        _, _, instance = self._mock_instance()
        routing = RoutingPolicyType.ANY
        description = "testing"

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
        )
        app_profile_pb = app_profile._to_pb()

        self.assertEqual(app_profile_pb.name, app_profile.name)
        expected_routing = instance_pb2.AppProfile.MultiClusterRoutingUseAny()
        self.assertEqual(
            app_profile_pb.multi_cluster_routing_use_any,
            expected_routing,
        )

    def test__to_pb_routing_single(self):
        from google.cloud.bigtable.enums import RoutingPolicyType
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        _, _, instance = self._mock_instance()
        routing = RoutingPolicyType.SINGLE
        description = "testing"

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
            cluster_id=self.CLUSTER_ID,
            allow_transactional_writes=True,
        )
        app_profile_pb = app_profile._to_pb()

        self.assertEqual(app_profile_pb.name, app_profile.name)
        expected_routing = instance_pb2.AppProfile.SingleClusterRouting(
            cluster_id=self.CLUSTER_ID, allow_transactional_writes=True
        )
        self.assertEqual(app_profile_pb.single_cluster_routing, expected_routing)

    def test__to_pb_w_wrong_routing_policy(self):
        _, _, instance = self._mock_instance()

        app_profile = self._make_one(
            self.APP_PROFILE_ID, instance, routing_policy_type=None
        )
        with self.assertRaises(ValueError):
            app_profile._to_pb()

    def test_reload(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2
        from google.cloud.bigtable.enums import RoutingPolicyType

        admin_api, _, instance = self._mock_instance()

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=RoutingPolicyType.ANY,
            description="original description",
        )

        # Create response_pb
        single_cluster_routing = instance_pb2.AppProfile.SingleClusterRouting(
            cluster_id=self.CLUSTER_ID,
            allow_transactional_writes=True,
        )
        response_pb = instance_pb2.AppProfile(
            name=app_profile.name,
            single_cluster_routing=single_cluster_routing,
            description="updated description",
        )
        admin_api.get_app_profile.return_value = response_pb

        self.assertIsNone(app_profile.reload())

        self.assertEqual(app_profile._to_pb(), response_pb)

    def test_exists_hit(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        admin_api, _, instance = self._mock_instance()

        response_pb = instance_pb2.AppProfile(name=self.APP_PROFILE_NAME)
        admin_api.get_app_profile.return_value = response_pb

        app_profile = self._make_one(self.APP_PROFILE_ID, instance)

        self.assertTrue(app_profile.exists())

    def test_exists_miss(self):
        from google.api_core import exceptions

        admin_api, _, instance = self._mock_instance()

        admin_api.get_app_profile.side_effect = [exceptions.NotFound("testing")]

        app_profile = self._make_one(self.APP_PROFILE_ID, instance)
        self.assertFalse(app_profile.exists())

    def test_exists_bad_request(self):
        from google.api_core import exceptions

        admin_api, _, instance = self._mock_instance()

        admin_api.get_app_profile.side_effect = [exceptions.BadRequest("testing")]

        # Perform the method and check the result.
        app_profile = self._make_one(self.APP_PROFILE_ID, instance)
        with self.assertRaises(exceptions.BadRequest):
            app_profile.exists()

    def _create_helper(self, routing, ignore_warnings=None):
        admin_api, client, instance = self._mock_instance()
        description = "testing"

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
        )
        app_profile_pb = app_profile._to_pb()
        admin_api.create_app_profile.return_value = app_profile_pb

        result = app_profile.create(ignore_warnings)

        self.assertEqual(result, app_profile)

        admin_api.create_app_profile.assert_called_once_with(
            parent=instance.name,
            app_profile_id=self.APP_PROFILE_ID,
            app_profile=app_profile_pb,
            ignore_warnings=ignore_warnings,
        )

    def test_create_w_routing_any_defaults(self):
        from google.cloud.bigtable.enums import RoutingPolicyType

        self._create_helper(RoutingPolicyType.ANY)

    def test_create_w_routing_single_w_ignore_warnings(self):
        from google.cloud.bigtable.enums import RoutingPolicyType

        self._create_helper(RoutingPolicyType.SINGLE, ignore_warnings=True)

    def _update_helper(self, routing, ignore_warnings=None):
        from google.protobuf import field_mask_pb2
        from google.cloud.bigtable.enums import RoutingPolicyType

        admin_api, client, instance = self._mock_instance()
        description = "testing"

        app_profile = self._make_one(
            self.APP_PROFILE_ID,
            instance,
            routing_policy_type=routing,
            description=description,
        )
        app_profile_pb = app_profile._to_pb()

        result = app_profile.update(ignore_warnings)

        self.assertIs(result, admin_api.update_app_profile.return_value)

        if routing is RoutingPolicyType.SINGLE:
            expected_update_mask = field_mask_pb2.FieldMask(
                paths=["description", "single_cluster_routing"]
            )
        elif routing is RoutingPolicyType.ANY:
            expected_update_mask = field_mask_pb2.FieldMask(
                paths=["description", "multi_cluster_routing_use_any"]
            )
        else:  # pragma: NO COVER
            self.fail(0, "Can't get here")

        admin_api.update_app_profile.assert_called_once_with(
            app_profile=app_profile_pb,
            update_mask=expected_update_mask,
            ignore_warnings=ignore_warnings,
        )

    def test_update_app_profile_routing_any(self):
        from google.cloud.bigtable.enums import RoutingPolicyType

        self._update_helper(routing=RoutingPolicyType.ANY, ignore_warnings=False)

    def test_update_app_profile_routing_single(self):
        from google.cloud.bigtable.enums import RoutingPolicyType

        self._update_helper(routing=RoutingPolicyType.SINGLE, ignore_warnings=True)

    def test_update_app_profile_with_wrong_routing_policy(self):
        admin_api, client, instance = self._mock_instance()

        app_profile = self._make_one(
            self.APP_PROFILE_ID, instance, routing_policy_type=None
        )

        with self.assertRaises(ValueError):
            app_profile.update()

    def _delete_helper(self, ignore_warnings=None):
        from google.protobuf import empty_pb2

        admin_api, client, instance = self._mock_instance()
        admin_api.delete_app_profile.return_value = empty_pb2.Empty()
        app_profile = self._make_one(self.APP_PROFILE_ID, instance)

        if ignore_warnings is not None:
            result = app_profile.delete(ignore_warnings=ignore_warnings)
        else:
            result = app_profile.delete()

        self.assertIsNone(result)

        admin_api.delete_app_profile.assert_called_once_with(
            self.APP_PROFILE_NAME,
            ignore_warnings=ignore_warnings,
        )

    def test_delete_w_default(self):
        self._delete_helper()

    def test_delete_w_ignore_warnings_true(self):
        self._delete_helper(ignore_warnings=True)
