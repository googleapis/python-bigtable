# Copyright 2025 Google LLC.
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

# TODO: Samples in docstrings will be slightly off. Fix this later. Also improve docstrings.

import abc

from google.protobuf import duration_pb2
from google.cloud.bigtable.admin_v2.types import table as table_pb2


class BaseGarbageCollectionRule(object, metaclass=abc.ABCMeta):
    """Garbage collection rule for column families within a table.

    Cells in the column family (within a table) fitting the rule will be
    deleted during garbage collection.

    .. note::

        This class is a do-nothing base class for all GC rules.

    .. note::

        A string ``gc_expression`` can also be used with API requests, but
        that value would be superceded by a ``gc_rule``. As a result, we
        don't support that feature and instead support via native classes.
    """
    
    @abc.abstractmethod
    def to_pb(self) -> table_pb2.GcRule:
        raise NotImplementedError()


class DefaultGarbageCollectionRule(BaseGarbageCollectionRule):
    """Default garbage collection rule.

    TODO: Add a sample for this.
    """

    def to_pb(self) -> table_pb2.GcRule:
        return table_pb2.GcRule()


class MaxNumVersionsGarbageCollectionRule(BaseGarbageCollectionRule):
    """Garbage collection limiting the number of versions of a cell.

    For example:

    .. literalinclude:: snippets_table.py
        :start-after: [START bigtable_api_create_family_gc_max_versions]
        :end-before: [END bigtable_api_create_family_gc_max_versions]
        :dedent: 4

    :type max_num_versions: int
    :param max_num_versions: The maximum number of versions
    """

    def __init__(self, max_num_versions: int):
        self.max_num_versions = max_num_versions

    def to_pb(self) -> table_pb2.GcRule:
        """Converts the garbage collection rule to a protobuf.

        :rtype: :class:`.table_v2_pb2.GcRule`
        :returns: The converted current object.
        """
        return table_pb2.GcRule(max_num_versions=self.max_num_versions)


class MaxAgeGarbageCollectionRule(BaseGarbageCollectionRule):
    """Garbage collection limiting the age of a cell.

    For example:

    .. literalinclude:: snippets_table.py
        :start-after: [START bigtable_api_create_family_gc_max_age]
        :end-before: [END bigtable_api_create_family_gc_max_age]
        :dedent: 4

    :type max_age: :class:`google.protobuf.duration_pb2.Duration`
    :param max_age: The maximum age allowed for a cell in the table.
    """

    def __init__(self, max_age: duration_pb2.Duration):
        self.max_age = max_age

    def to_pb(self) -> table_pb2.GcRule:
        """Converts the garbage collection rule to a protobuf.

        :rtype: :class:`.table_v2_pb2.GcRule`
        :returns: The converted current object.
        """
        return table_pb2.GcRule(max_age=self.max_age)


class UnionGarbageCollectionRule(BaseGarbageCollectionRule):
    """Union of garbage collection rules.

    For example:

    .. literalinclude:: snippets_table.py
        :start-after: [START bigtable_api_create_family_gc_union]
        :end-before: [END bigtable_api_create_family_gc_union]
        :dedent: 4

    :type rules: list
    :param rules: List of :class:`GarbageCollectionRule`.
    """

    def __init__(self, rules):
        self.rules = rules

    def to_pb(self) -> table_pb2.GcRule:
        """Converts the union into a single GC rule as a protobuf.

        :rtype: :class:`.table_v2_pb2.GcRule`
        :returns: The converted current object.
        """
        union = table_pb2.GcRule.Union(rules=[rule.to_pb() for rule in self.rules])
        return table_pb2.GcRule(union=union)


class IntersectionGarbageCollectionRule(BaseGarbageCollectionRule):
    """Intersection of garbage collection rules.

    For example:

    .. literalinclude:: snippets_table.py
        :start-after: [START bigtable_api_create_family_gc_intersection]
        :end-before: [END bigtable_api_create_family_gc_intersection]
        :dedent: 4

    :type rules: list
    :param rules: List of :class:`GarbageCollectionRule`.
    """

    def __init__(self, rules):
        self.rules = rules

    def to_pb(self) -> table_pb2.GcRule:
        """Converts the intersection into a single GC rule as a protobuf.

        :rtype: :class:`.table_v2_pb2.GcRule`
        :returns: The converted current object.
        """
        intersection = table_pb2.GcRule.Intersection(
            rules=[rule.to_pb() for rule in self.rules]
        )
        return table_pb2.GcRule(intersection=intersection)
