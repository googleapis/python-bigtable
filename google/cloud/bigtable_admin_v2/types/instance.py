# -*- coding: utf-8 -*-
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

from typing import MutableMapping, MutableSequence

import proto  # type: ignore

from google.cloud.bigtable_admin_v2.types import common
from google.protobuf import timestamp_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.bigtable.admin.v2",
    manifest={
        "Instance",
        "AutoscalingTargets",
        "AutoscalingLimits",
        "Cluster",
        "AppProfile",
        "HotTablet",
    },
)


class Instance(proto.Message):
    r"""A collection of Bigtable [Tables][google.bigtable.admin.v2.Table]
    and the resources that serve them. All tables in an instance are
    served from all [Clusters][google.bigtable.admin.v2.Cluster] in the
    instance.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        name (str):
            The unique name of the instance. Values are of the form
            ``projects/{project}/instances/[a-z][a-z0-9\\-]+[a-z0-9]``.
        display_name (str):
            Required. The descriptive name for this
            instance as it appears in UIs. Can be changed at
            any time, but should be kept globally unique to
            avoid confusion.
        state (google.cloud.bigtable_admin_v2.types.Instance.State):
            (``OutputOnly``) The current state of the instance.
        type_ (google.cloud.bigtable_admin_v2.types.Instance.Type):
            The type of the instance. Defaults to ``PRODUCTION``.
        labels (MutableMapping[str, str]):
            Labels are a flexible and lightweight mechanism for
            organizing cloud resources into groups that reflect a
            customer's organizational needs and deployment strategies.
            They can be used to filter resources and aggregate metrics.

            -  Label keys must be between 1 and 63 characters long and
               must conform to the regular expression:
               ``[\p{Ll}\p{Lo}][\p{Ll}\p{Lo}\p{N}_-]{0,62}``.
            -  Label values must be between 0 and 63 characters long and
               must conform to the regular expression:
               ``[\p{Ll}\p{Lo}\p{N}_-]{0,63}``.
            -  No more than 64 labels can be associated with a given
               resource.
            -  Keys and values must both be under 128 bytes.
        create_time (google.protobuf.timestamp_pb2.Timestamp):
            Output only. A server-assigned timestamp representing when
            this Instance was created. For instances created before this
            field was added (August 2021), this value is
            ``seconds: 0, nanos: 1``.
        satisfies_pzs (bool):
            Output only. Reserved for future use.

            This field is a member of `oneof`_ ``_satisfies_pzs``.
    """

    class State(proto.Enum):
        r"""Possible states of an instance.

        Values:
            STATE_NOT_KNOWN (0):
                The state of the instance could not be
                determined.
            READY (1):
                The instance has been successfully created
                and can serve requests to its tables.
            CREATING (2):
                The instance is currently being created, and
                may be destroyed if the creation process
                encounters an error.
        """
        STATE_NOT_KNOWN = 0
        READY = 1
        CREATING = 2

    class Type(proto.Enum):
        r"""The type of the instance.

        Values:
            TYPE_UNSPECIFIED (0):
                The type of the instance is unspecified. If set when
                creating an instance, a ``PRODUCTION`` instance will be
                created. If set when updating an instance, the type will be
                left unchanged.
            PRODUCTION (1):
                An instance meant for production use. ``serve_nodes`` must
                be set on the cluster.
            DEVELOPMENT (2):
                DEPRECATED: Prefer PRODUCTION for all use
                cases, as it no longer enforces
                a higher minimum node count than DEVELOPMENT.
        """
        TYPE_UNSPECIFIED = 0
        PRODUCTION = 1
        DEVELOPMENT = 2

    name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    display_name: str = proto.Field(
        proto.STRING,
        number=2,
    )
    state: State = proto.Field(
        proto.ENUM,
        number=3,
        enum=State,
    )
    type_: Type = proto.Field(
        proto.ENUM,
        number=4,
        enum=Type,
    )
    labels: MutableMapping[str, str] = proto.MapField(
        proto.STRING,
        proto.STRING,
        number=5,
    )
    create_time: timestamp_pb2.Timestamp = proto.Field(
        proto.MESSAGE,
        number=7,
        message=timestamp_pb2.Timestamp,
    )
    satisfies_pzs: bool = proto.Field(
        proto.BOOL,
        number=8,
        optional=True,
    )


class AutoscalingTargets(proto.Message):
    r"""The Autoscaling targets for a Cluster. These determine the
    recommended nodes.

    Attributes:
        cpu_utilization_percent (int):
            The cpu utilization that the Autoscaler should be trying to
            achieve. This number is on a scale from 0 (no utilization)
            to 100 (total utilization), and is limited between 10 and
            80, otherwise it will return INVALID_ARGUMENT error.
        storage_utilization_gib_per_node (int):
            The storage utilization that the Autoscaler should be trying
            to achieve. This number is limited between 2560 (2.5TiB) and
            5120 (5TiB) for a SSD cluster and between 8192 (8TiB) and
            16384 (16TiB) for an HDD cluster; otherwise it will return
            INVALID_ARGUMENT error. If this value is set to 0, it will
            be treated as if it were set to the default value: 2560 for
            SSD, 8192 for HDD.
    """

    cpu_utilization_percent: int = proto.Field(
        proto.INT32,
        number=2,
    )
    storage_utilization_gib_per_node: int = proto.Field(
        proto.INT32,
        number=3,
    )


class AutoscalingLimits(proto.Message):
    r"""Limits for the number of nodes a Cluster can autoscale
    up/down to.

    Attributes:
        min_serve_nodes (int):
            Required. Minimum number of nodes to scale
            down to.
        max_serve_nodes (int):
            Required. Maximum number of nodes to scale up
            to.
    """

    min_serve_nodes: int = proto.Field(
        proto.INT32,
        number=1,
    )
    max_serve_nodes: int = proto.Field(
        proto.INT32,
        number=2,
    )


class Cluster(proto.Message):
    r"""A resizable group of nodes in a particular cloud location, capable
    of serving all [Tables][google.bigtable.admin.v2.Table] in the
    parent [Instance][google.bigtable.admin.v2.Instance].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        name (str):
            The unique name of the cluster. Values are of the form
            ``projects/{project}/instances/{instance}/clusters/[a-z][-a-z0-9]*``.
        location (str):
            Immutable. The location where this cluster's nodes and
            storage reside. For best performance, clients should be
            located as close as possible to this cluster. Currently only
            zones are supported, so values should be of the form
            ``projects/{project}/locations/{zone}``.
        state (google.cloud.bigtable_admin_v2.types.Cluster.State):
            Output only. The current state of the
            cluster.
        serve_nodes (int):
            The number of nodes allocated to this
            cluster. More nodes enable higher throughput and
            more consistent performance.
        cluster_config (google.cloud.bigtable_admin_v2.types.Cluster.ClusterConfig):
            Configuration for this cluster.

            This field is a member of `oneof`_ ``config``.
        default_storage_type (google.cloud.bigtable_admin_v2.types.StorageType):
            Immutable. The type of storage used by this
            cluster to serve its parent instance's tables,
            unless explicitly overridden.
        encryption_config (google.cloud.bigtable_admin_v2.types.Cluster.EncryptionConfig):
            Immutable. The encryption configuration for
            CMEK-protected clusters.
    """

    class State(proto.Enum):
        r"""Possible states of a cluster.

        Values:
            STATE_NOT_KNOWN (0):
                The state of the cluster could not be
                determined.
            READY (1):
                The cluster has been successfully created and
                is ready to serve requests.
            CREATING (2):
                The cluster is currently being created, and
                may be destroyed if the creation process
                encounters an error. A cluster may not be able
                to serve requests while being created.
            RESIZING (3):
                The cluster is currently being resized, and
                may revert to its previous node count if the
                process encounters an error. A cluster is still
                capable of serving requests while being resized,
                but may exhibit performance as if its number of
                allocated nodes is between the starting and
                requested states.
            DISABLED (4):
                The cluster has no backing nodes. The data
                (tables) still exist, but no operations can be
                performed on the cluster.
        """
        STATE_NOT_KNOWN = 0
        READY = 1
        CREATING = 2
        RESIZING = 3
        DISABLED = 4

    class ClusterAutoscalingConfig(proto.Message):
        r"""Autoscaling config for a cluster.

        Attributes:
            autoscaling_limits (google.cloud.bigtable_admin_v2.types.AutoscalingLimits):
                Required. Autoscaling limits for this
                cluster.
            autoscaling_targets (google.cloud.bigtable_admin_v2.types.AutoscalingTargets):
                Required. Autoscaling targets for this
                cluster.
        """

        autoscaling_limits: "AutoscalingLimits" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="AutoscalingLimits",
        )
        autoscaling_targets: "AutoscalingTargets" = proto.Field(
            proto.MESSAGE,
            number=2,
            message="AutoscalingTargets",
        )

    class ClusterConfig(proto.Message):
        r"""Configuration for a cluster.

        Attributes:
            cluster_autoscaling_config (google.cloud.bigtable_admin_v2.types.Cluster.ClusterAutoscalingConfig):
                Autoscaling configuration for this cluster.
        """

        cluster_autoscaling_config: "Cluster.ClusterAutoscalingConfig" = proto.Field(
            proto.MESSAGE,
            number=1,
            message="Cluster.ClusterAutoscalingConfig",
        )

    class EncryptionConfig(proto.Message):
        r"""Cloud Key Management Service (Cloud KMS) settings for a
        CMEK-protected cluster.

        Attributes:
            kms_key_name (str):
                Describes the Cloud KMS encryption key that will be used to
                protect the destination Bigtable cluster. The requirements
                for this key are:

                1) The Cloud Bigtable service account associated with the
                   project that contains this cluster must be granted the
                   ``cloudkms.cryptoKeyEncrypterDecrypter`` role on the CMEK
                   key.
                2) Only regional keys can be used and the region of the CMEK
                   key must match the region of the cluster.
                3) All clusters within an instance must use the same CMEK
                   key. Values are of the form
                   ``projects/{project}/locations/{location}/keyRings/{keyring}/cryptoKeys/{key}``
        """

        kms_key_name: str = proto.Field(
            proto.STRING,
            number=1,
        )

    name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    location: str = proto.Field(
        proto.STRING,
        number=2,
    )
    state: State = proto.Field(
        proto.ENUM,
        number=3,
        enum=State,
    )
    serve_nodes: int = proto.Field(
        proto.INT32,
        number=4,
    )
    cluster_config: ClusterConfig = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="config",
        message=ClusterConfig,
    )
    default_storage_type: common.StorageType = proto.Field(
        proto.ENUM,
        number=5,
        enum=common.StorageType,
    )
    encryption_config: EncryptionConfig = proto.Field(
        proto.MESSAGE,
        number=6,
        message=EncryptionConfig,
    )


class AppProfile(proto.Message):
    r"""A configuration object describing how Cloud Bigtable should
    treat traffic from a particular end user application.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        name (str):
            The unique name of the app profile. Values are of the form
            ``projects/{project}/instances/{instance}/appProfiles/[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.
        etag (str):
            Strongly validated etag for optimistic concurrency control.
            Preserve the value returned from ``GetAppProfile`` when
            calling ``UpdateAppProfile`` to fail the request if there
            has been a modification in the mean time. The
            ``update_mask`` of the request need not include ``etag`` for
            this protection to apply. See
            `Wikipedia <https://en.wikipedia.org/wiki/HTTP_ETag>`__ and
            `RFC
            7232 <https://tools.ietf.org/html/rfc7232#section-2.3>`__
            for more details.
        description (str):
            Long form description of the use case for
            this AppProfile.
        multi_cluster_routing_use_any (google.cloud.bigtable_admin_v2.types.AppProfile.MultiClusterRoutingUseAny):
            Use a multi-cluster routing policy.

            This field is a member of `oneof`_ ``routing_policy``.
        single_cluster_routing (google.cloud.bigtable_admin_v2.types.AppProfile.SingleClusterRouting):
            Use a single-cluster routing policy.

            This field is a member of `oneof`_ ``routing_policy``.
    """

    class MultiClusterRoutingUseAny(proto.Message):
        r"""Read/write requests are routed to the nearest cluster in the
        instance, and will fail over to the nearest cluster that is
        available in the event of transient errors or delays. Clusters
        in a region are considered equidistant. Choosing this option
        sacrifices read-your-writes consistency to improve availability.

        Attributes:
            cluster_ids (MutableSequence[str]):
                The set of clusters to route to. The order is
                ignored; clusters will be tried in order of
                distance. If left empty, all clusters are
                eligible.
        """

        cluster_ids: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=1,
        )

    class SingleClusterRouting(proto.Message):
        r"""Unconditionally routes all read/write requests to a specific
        cluster. This option preserves read-your-writes consistency but
        does not improve availability.

        Attributes:
            cluster_id (str):
                The cluster to which read/write requests
                should be routed.
            allow_transactional_writes (bool):
                Whether or not ``CheckAndMutateRow`` and
                ``ReadModifyWriteRow`` requests are allowed by this app
                profile. It is unsafe to send these requests to the same
                table/row/column in multiple clusters.
        """

        cluster_id: str = proto.Field(
            proto.STRING,
            number=1,
        )
        allow_transactional_writes: bool = proto.Field(
            proto.BOOL,
            number=2,
        )

    name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    etag: str = proto.Field(
        proto.STRING,
        number=2,
    )
    description: str = proto.Field(
        proto.STRING,
        number=3,
    )
    multi_cluster_routing_use_any: MultiClusterRoutingUseAny = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="routing_policy",
        message=MultiClusterRoutingUseAny,
    )
    single_cluster_routing: SingleClusterRouting = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="routing_policy",
        message=SingleClusterRouting,
    )


class HotTablet(proto.Message):
    r"""A tablet is a defined by a start and end key and is explained
    in https://cloud.google.com/bigtable/docs/overview#architecture
    and
    https://cloud.google.com/bigtable/docs/performance#optimization.
    A Hot tablet is a tablet that exhibits high average cpu usage
    during the time interval from start time to end time.

    Attributes:
        name (str):
            The unique name of the hot tablet. Values are of the form
            ``projects/{project}/instances/{instance}/clusters/{cluster}/hotTablets/[a-zA-Z0-9_-]*``.
        table_name (str):
            Name of the table that contains the tablet. Values are of
            the form
            ``projects/{project}/instances/{instance}/tables/[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.
        start_time (google.protobuf.timestamp_pb2.Timestamp):
            Output only. The start time of the hot
            tablet.
        end_time (google.protobuf.timestamp_pb2.Timestamp):
            Output only. The end time of the hot tablet.
        start_key (str):
            Tablet Start Key (inclusive).
        end_key (str):
            Tablet End Key (inclusive).
        node_cpu_usage_percent (float):
            Output only. The average CPU usage spent by a node on this
            tablet over the start_time to end_time time range. The
            percentage is the amount of CPU used by the node to serve
            the tablet, from 0% (tablet was not interacted with) to 100%
            (the node spent all cycles serving the hot tablet).
    """

    name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    table_name: str = proto.Field(
        proto.STRING,
        number=2,
    )
    start_time: timestamp_pb2.Timestamp = proto.Field(
        proto.MESSAGE,
        number=3,
        message=timestamp_pb2.Timestamp,
    )
    end_time: timestamp_pb2.Timestamp = proto.Field(
        proto.MESSAGE,
        number=4,
        message=timestamp_pb2.Timestamp,
    )
    start_key: str = proto.Field(
        proto.STRING,
        number=5,
    )
    end_key: str = proto.Field(
        proto.STRING,
        number=6,
    )
    node_cpu_usage_percent: float = proto.Field(
        proto.FLOAT,
        number=7,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
