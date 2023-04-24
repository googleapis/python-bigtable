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
#
from __future__ import annotations
from typing import TYPE_CHECKING, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from google.cloud.bigtable import RowKeySamples


class Mutation(ABC):
    """Model class for mutations"""

    @abstractmethod
    def _to_dict(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass
class SetCell(Mutation):
    family:str
    qualifier:bytes
    new_value:bytes|str|int
    timestamp_micros:int|None

    def _to_dict(self) -> dict[str, Any]:
        return {
            "set_cell": {
                "family_name": self.family,
                "column_qualifier": self.qualifier,
                "timestamp_micros": self.timestamp_micros if self.timestamp_micros is not None else -1,
                "value": self.new_value,
            }
        }


@dataclass
class DeleteRangeFromColumn(Mutation):
    family:str
    qualifier:bytes
    # None represents 0
    start_timestamp_micros:int|None
    # None represents infinity
    end_timestamp_micros:int|None

    def _to_dict(self) -> dict[str, Any]:
        timestamp_range = {}
        if self.start_timestamp_micros is not None:
            timestamp_range["start_timestamp_micros"] = self.start_timestamp_micros
        if self.end_timestamp_micros is not None:
            timestamp_range["end_timestamp_micros"] = self.end_timestamp_micros
        return {
            "delete_from_column": {
                "family_name": self.family,
                "column_qualifier": self.qualifier,
                "time_range": timestamp_range,
            }
        }

@dataclass
class DeleteAllFromFamily(Mutation):
    family_to_delete:str

    def _to_dict(self) -> dict[str, Any]:
        return {
            "delete_from_family": {
                "family_name": self.family_to_delete,
            }
        }


@dataclass
class DeleteAllFromRow(Mutation):

    def _to_dict(self) -> dict[str, Any]:
        return {
            "delete_from_row": {},
        }


@dataclass
class BulkMutationsEntry():
    row_key:bytes
    mutations: list[Mutation]|Mutation

    def _to_dict(self) -> dict[str, Any]:
        return {
            "row_key": self.row_key,
            "mutations": [mutation._to_dict() for mutation in self.mutations]
        }
