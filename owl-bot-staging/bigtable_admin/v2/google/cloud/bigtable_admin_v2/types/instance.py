# -*- coding: utf-8 -*-
# Copyright 2020 Google LLC
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
import proto  # type: ignore

from google.cloud.bigtable_admin_v2.types import common


__protobuf__ = proto.module(
    package='google.bigtable.admin.v2',
    manifest={
        'Instance',
        'Cluster',
        'AppProfile',
    },
)


class Instance(proto.Message):
    r"""A collection of Bigtable [Tables][google.bigtable.admin.v2.Table]
    and the resources that serve them. All tables in an instance are
    served from all [Clusters][google.bigtable.admin.v2.Cluster] in the
    instance.

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
        labels (Sequence[google.cloud.bigtable_admin_v2.types.Instance.LabelsEntry]):
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
    """
    class State(proto.Enum):
        r"""Possible states of an instance."""
        STATE_NOT_KNOWN = 0
        READY = 1
        CREATING = 2

    class Type(proto.Enum):
        r"""The type of the instance."""
        TYPE_UNSPECIFIED = 0
        PRODUCTION = 1
        DEVELOPMENT = 2

    name = proto.Field(
        proto.STRING,
        number=1,
    )
    display_name = proto.Field(
        proto.STRING,
        number=2,
    )
    state = proto.Field(
        proto.ENUM,
        number=3,
        enum=State,
    )
    type_ = proto.Field(
        proto.ENUM,
        number=4,
        enum=Type,
    )
    labels = proto.MapField(
        proto.STRING,
        proto.STRING,
        number=5,
    )


class Cluster(proto.Message):
    r"""A resizable group of nodes in a particular cloud location, capable
    of serving all [Tables][google.bigtable.admin.v2.Table] in the
    parent [Instance][google.bigtable.admin.v2.Instance].

    Attributes:
        name (str):
            The unique name of the cluster. Values are of the form
            ``projects/{project}/instances/{instance}/clusters/[a-z][-a-z0-9]*``.
        location (str):
            (``CreationOnly``) The location where this cluster's nodes
            and storage reside. For best performance, clients should be
            located as close as possible to this cluster. Currently only
            zones are supported, so values should be of the form
            ``projects/{project}/locations/{zone}``.
        state (google.cloud.bigtable_admin_v2.types.Cluster.State):
            The current state of the cluster.
        serve_nodes (int):
            Required. The number of nodes allocated to
            this cluster. More nodes enable higher
            throughput and more consistent performance.
        default_storage_type (google.cloud.bigtable_admin_v2.types.StorageType):
            (``CreationOnly``) The type of storage used by this cluster
            to serve its parent instance's tables, unless explicitly
            overridden.
        encryption_config (google.cloud.bigtable_admin_v2.types.Cluster.EncryptionConfig):
            Immutable. The encryption configuration for
            CMEK-protected clusters.
    """
    class State(proto.Enum):
        r"""Possible states of a cluster."""
        STATE_NOT_KNOWN = 0
        READY = 1
        CREATING = 2
        RESIZING = 3
        DISABLED = 4

    class EncryptionConfig(proto.Message):
        r"""Cloud Key Management Service (Cloud KMS) settings for a CMEK-
        rotected cluster.

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
                   key.
        """

        kms_key_name = proto.Field(
            proto.STRING,
            number=1,
        )

    name = proto.Field(
        proto.STRING,
        number=1,
    )
    location = proto.Field(
        proto.STRING,
        number=2,
    )
    state = proto.Field(
        proto.ENUM,
        number=3,
        enum=State,
    )
    serve_nodes = proto.Field(
        proto.INT32,
        number=4,
    )
    default_storage_type = proto.Field(
        proto.ENUM,
        number=5,
        enum=common.StorageType,
    )
    encryption_config = proto.Field(
        proto.MESSAGE,
        number=6,
        message=EncryptionConfig,
    )


class AppProfile(proto.Message):
    r"""A configuration object describing how Cloud Bigtable should
    treat traffic from a particular end user application.

    Attributes:
        name (str):
            (``OutputOnly``) The unique name of the app profile. Values
            are of the form
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
            Optional long form description of the use
            case for this AppProfile.
        multi_cluster_routing_use_any (google.cloud.bigtable_admin_v2.types.AppProfile.MultiClusterRoutingUseAny):
            Use a multi-cluster routing policy.
        single_cluster_routing (google.cloud.bigtable_admin_v2.types.AppProfile.SingleClusterRouting):
            Use a single-cluster routing policy.
    """

    class MultiClusterRoutingUseAny(proto.Message):
        r"""Read/write requests are routed to the nearest cluster in the
        instance, and will fail over to the nearest cluster that is
        available in the event of transient errors or delays. Clusters
        in a region are considered equidistant. Choosing this option
        sacrifices read-your-writes consistency to improve availability.
            """

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

        cluster_id = proto.Field(
            proto.STRING,
            number=1,
        )
        allow_transactional_writes = proto.Field(
            proto.BOOL,
            number=2,
        )

    name = proto.Field(
        proto.STRING,
        number=1,
    )
    etag = proto.Field(
        proto.STRING,
        number=2,
    )
    description = proto.Field(
        proto.STRING,
        number=3,
    )
    multi_cluster_routing_use_any = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof='routing_policy',
        message=MultiClusterRoutingUseAny,
    )
    single_cluster_routing = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof='routing_policy',
        message=SingleClusterRouting,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
