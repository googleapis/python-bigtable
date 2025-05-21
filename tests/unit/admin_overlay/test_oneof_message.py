# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
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
from google.cloud.bigtable.admin_v2.types import GcRule
from google.protobuf import duration_pb2

import my_oneof_message

import unittest

# The following proto bytestring was constructed running printproto in
# text-to-binary mode on the following textproto for GcRule:
#
# intersection {
#   rules {
#     max_num_versions: 1234
#   }
#   rules {
#     max_age {
#       seconds: 12345
#     }
#   }
# }
GCRULE_RAW_PROTO_BYTESTRING = b"\x1a\x0c\n\x03\x08\xd2\t\n\x05\x12\x03\x08\xb9`"
INITIAL_VALUE = 123
FINAL_VALUE = 456


class TestGenericOneofMessage(unittest.TestCase):
    def setUp(self):
        self.default_msg = my_oneof_message.MyOneofMessage()
        self.foo_msg = my_oneof_message.MyOneofMessage(foo=INITIAL_VALUE)

    def test_setattr_oneof_no_conflict(self):
        self.default_msg.foo = INITIAL_VALUE
        self.default_msg.baz = INITIAL_VALUE
        self.assertEqual(self.default_msg.foo, INITIAL_VALUE)
        self.assertEqual(self.default_msg.baz, INITIAL_VALUE)
        self.assertFalse(self.default_msg.bar)

    def test_setattr_oneof_conflict(self):
        with self.assertRaises(ValueError):
            self.foo_msg.bar = INITIAL_VALUE
        self.assertEqual(self.foo_msg.foo, INITIAL_VALUE)
        self.assertFalse(self.foo_msg.bar)

        self.default_msg.bar = INITIAL_VALUE
        with self.assertRaises(ValueError):
            self.default_msg.foo = INITIAL_VALUE
        self.assertEqual(self.default_msg.bar, INITIAL_VALUE)
        self.assertFalse(self.default_msg.foo)

    def test_setattr_oneof_same_oneof_field(self):
        self.foo_msg.foo = FINAL_VALUE
        self.assertEqual(self.foo_msg.foo, FINAL_VALUE)
        self.assertFalse(self.foo_msg.bar)

        self.default_msg.foo = INITIAL_VALUE
        self.default_msg.foo = FINAL_VALUE
        self.assertEqual(self.default_msg.foo, FINAL_VALUE)

    def test_setattr_oneof_delattr(self):
        del self.foo_msg.foo
        self.foo_msg.bar = INITIAL_VALUE
        self.assertEqual(self.foo_msg.bar, INITIAL_VALUE)
        self.assertFalse(self.foo_msg.foo)

    def test_init_oneof_conflict(self):
        with self.assertRaises(ValueError):
            my_oneof_message.MyOneofMessage(foo=INITIAL_VALUE, bar=INITIAL_VALUE)

        with self.assertRaises(ValueError):
            my_oneof_message.MyOneofMessage(
                {
                    "foo": INITIAL_VALUE,
                    "bar": INITIAL_VALUE,
                }
            )

        with self.assertRaises(ValueError):
            my_oneof_message.MyOneofMessage(self.foo_msg._pb, bar=INITIAL_VALUE)

        with self.assertRaises(ValueError):
            my_oneof_message.MyOneofMessage(self.foo_msg, bar=INITIAL_VALUE)

    def test_init_oneof_no_conflict(self):
        msg = my_oneof_message.MyOneofMessage(foo=INITIAL_VALUE, baz=INITIAL_VALUE)
        self.assertEqual(msg.foo, INITIAL_VALUE)
        self.assertEqual(msg.baz, INITIAL_VALUE)
        self.assertFalse(msg.bar)

        msg = my_oneof_message.MyOneofMessage(
            {
                "foo": INITIAL_VALUE,
                "baz": INITIAL_VALUE,
            }
        )
        self.assertEqual(msg.foo, INITIAL_VALUE)
        self.assertEqual(msg.baz, INITIAL_VALUE)
        self.assertFalse(msg.bar)

        msg = my_oneof_message.MyOneofMessage(self.foo_msg, baz=INITIAL_VALUE)
        self.assertEqual(msg.foo, INITIAL_VALUE)
        self.assertEqual(msg.baz, INITIAL_VALUE)
        self.assertFalse(msg.bar)

        msg = my_oneof_message.MyOneofMessage(self.foo_msg._pb, baz=INITIAL_VALUE)
        self.assertEqual(msg.foo, INITIAL_VALUE)
        self.assertEqual(msg.baz, INITIAL_VALUE)
        self.assertFalse(msg.bar)

    def test_init_kwargs_override_same_field_oneof(self):
        # Kwargs take precedence over mapping, and this should be OK
        msg = my_oneof_message.MyOneofMessage(
            {
                "foo": INITIAL_VALUE,
            },
            foo=FINAL_VALUE,
        )
        self.assertEqual(msg.foo, FINAL_VALUE)

        msg = my_oneof_message.MyOneofMessage(self.foo_msg, foo=FINAL_VALUE)
        self.assertEqual(msg.foo, FINAL_VALUE)

        msg = my_oneof_message.MyOneofMessage(self.foo_msg._pb, foo=FINAL_VALUE)
        self.assertEqual(msg.foo, FINAL_VALUE)


class TestGcRule(unittest.TestCase):
    def test_serialize_deserialize(self):
        test = GcRule(
            intersection=GcRule.Intersection(
                rules=[
                    GcRule(max_num_versions=1234),
                    GcRule(max_age=duration_pb2.Duration(seconds=12345)),
                ]
            )
        )
        self.assertEqual(GcRule.serialize(test), GCRULE_RAW_PROTO_BYTESTRING)
        self.assertEqual(GcRule.deserialize(GCRULE_RAW_PROTO_BYTESTRING), test)


if __name__ == "__main__":
    unittest.main()
