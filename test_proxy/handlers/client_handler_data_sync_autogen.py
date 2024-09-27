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

# This file is automatically generated by CrossSync. Do not edit manually.

"""
This module contains the client handler process for proxy_server.py.
"""
import os
from google.cloud.environment_vars import BIGTABLE_EMULATOR
from google.cloud.bigtable.data._cross_sync import CrossSync
from client_handler_data_async import error_safe


class TestProxyClientHandler:
    """
    Implements the same methods as the grpc server, but handles the client
    library side of the request.

    Requests received in TestProxyGrpcServer are converted to a dictionary,
    and supplied to the TestProxyClientHandler methods as kwargs.
    The client response is then returned back to the TestProxyGrpcServer
    """

    def __init__(
        self,
        data_target=None,
        project_id=None,
        instance_id=None,
        app_profile_id=None,
        per_operation_timeout=None,
        **kwargs
    ):
        self.closed = False
        os.environ[BIGTABLE_EMULATOR] = data_target
        self.client = CrossSync._Sync_Impl.DataClient(project=project_id)
        self.instance_id = instance_id
        self.app_profile_id = app_profile_id
        self.per_operation_timeout = per_operation_timeout

    def close(self):
        self.closed = True

    @error_safe
    async def ReadRows(self, request, **kwargs):
        table_id = request.pop("table_name").split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = (
            kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        )
        result_list = table.read_rows(request, **kwargs)
        serialized_response = [row._to_dict() for row in result_list]
        return serialized_response

    @error_safe
    async def ReadRow(self, row_key, **kwargs):
        table_id = kwargs.pop("table_name").split("/")[-1]
        app_profile_id = self.app_profile_id or kwargs.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = (
            kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        )
        result_row = table.read_row(row_key, **kwargs)
        if result_row:
            return result_row._to_dict()
        else:
            return "None"

    @error_safe
    async def MutateRow(self, request, **kwargs):
        from google.cloud.bigtable.data.mutations import Mutation

        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = (
            kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        )
        row_key = request["row_key"]
        mutations = [Mutation._from_dict(d) for d in request["mutations"]]
        table.mutate_row(row_key, mutations, **kwargs)
        return "OK"

    @error_safe
    async def BulkMutateRows(self, request, **kwargs):
        from google.cloud.bigtable.data.mutations import RowMutationEntry

        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = (
            kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        )
        entry_list = [
            RowMutationEntry._from_dict(entry) for entry in request["entries"]
        ]
        table.bulk_mutate_rows(entry_list, **kwargs)
        return "OK"

    @error_safe
    async def CheckAndMutateRow(self, request, **kwargs):
        from google.cloud.bigtable.data.mutations import Mutation, SetCell

        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = (
            kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        )
        row_key = request["row_key"]
        true_mutations = []
        for mut_dict in request.get("true_mutations", []):
            try:
                true_mutations.append(Mutation._from_dict(mut_dict))
            except ValueError:
                mutation = SetCell("", "", "", 0)
                true_mutations.append(mutation)
        false_mutations = []
        for mut_dict in request.get("false_mutations", []):
            try:
                false_mutations.append(Mutation._from_dict(mut_dict))
            except ValueError:
                false_mutations.append(SetCell("", "", "", 0))
        predicate_filter = request.get("predicate_filter", None)
        result = table.check_and_mutate_row(
            row_key,
            predicate_filter,
            true_case_mutations=true_mutations,
            false_case_mutations=false_mutations,
            **kwargs
        )
        return result

    @error_safe
    async def ReadModifyWriteRow(self, request, **kwargs):
        from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule
        from google.cloud.bigtable.data.read_modify_write_rules import AppendValueRule

        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = (
            kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        )
        row_key = request["row_key"]
        rules = []
        for rule_dict in request.get("rules", []):
            qualifier = rule_dict["column_qualifier"]
            if "append_value" in rule_dict:
                new_rule = AppendValueRule(
                    rule_dict["family_name"], qualifier, rule_dict["append_value"]
                )
            else:
                new_rule = IncrementRule(
                    rule_dict["family_name"], qualifier, rule_dict["increment_amount"]
                )
            rules.append(new_rule)
        result = table.read_modify_write_row(row_key, rules, **kwargs)
        if result:
            return result._to_dict()
        else:
            return "None"

    @error_safe
    async def SampleRowKeys(self, request, **kwargs):
        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = (
            kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        )
        result = table.sample_row_keys(**kwargs)
        return result
