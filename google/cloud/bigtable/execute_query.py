# Copyright 2024 Google LLC
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

from typing import Optional, List, Dict, Any, Union, Iterable
from collections import defaultdict


class _NamedList:
    """This class is different from namedtuple, because namedtuple has some restrictions on names of fields and we do not want to have them."""

    def __init__(
        self, values: Iterable[Any], field_mapping: Dict[str, Union[int, Exception]]
    ):
        self._fields = [value for value in values]
        self._field_mapping = field_mapping

    @staticmethod
    def _construct_field_mapping(
        field_names: List[Optional[str]],
    ) -> Dict[str, Union[int, Exception]]:
        result = {}
        field_indexes = defaultdict(list)
        duplicate_names = set()
        for index, name in enumerate(field_names):
            if name is not None:
                result[name] = index
                field_indexes[name].append(index)
                if len(field_indexes[name]) > 1:
                    duplicate_names.add(name)

        for name in duplicate_names:
            result[name] = KeyError(
                f"Ambigious field name: '{name}', use index instead."
                f" Field present on indexes {', '.join(map(str, field_indexes[name]))}."
            )

        return result

    def __getitem__(self, index_or_name: Union[str, int]):
        if isinstance(index_or_name, str):
            index_or_exception = self._field_mapping[index_or_name]
            if isinstance(index_or_exception, Exception):
                raise index_or_exception
            index = index_or_exception
        else:
            index = index_or_name
        return self._fields[index]

    def __len__(self):
        return len(self._fields)


class QueryResultRow(_NamedList):
    pass


class Struct(_NamedList):
    pass
