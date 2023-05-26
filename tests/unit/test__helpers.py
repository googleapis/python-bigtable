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

import pytest
import google.cloud.bigtable._helpers as _helpers
import google.cloud.bigtable.exceptions as bigtable_exceptions

import mock


class TestMakeMetadata:
    @pytest.mark.parametrize(
        "table,profile,expected",
        [
            ("table", "profile", "table_name=table,app_profile_id=profile"),
            (None, "profile", "app_profile_id=profile"),
            ("table", None, "table_name=table"),
            (None, None, ""),
        ],
    )
    def test__make_metadata(self, table, profile, expected):
        metadata = _helpers._make_metadata(table, profile)
        assert metadata == [("x-goog-request-params", expected)]


class TestAttemptTimeoutGenerator:
    @pytest.mark.parametrize(
        "request_t,operation_t,expected_list",
        [
            (1, 3.5, [1, 1, 1, 0.5, 0, 0]),
            (None, 3.5, [3.5, 2.5, 1.5, 0.5, 0, 0]),
            (10, 5, [5, 4, 3, 2, 1, 0, 0]),
            (3, 3, [3, 2, 1, 0, 0, 0, 0]),
            (0, 3, [0, 0, 0]),
            (3, 0, [0, 0, 0]),
            (-1, 3, [0, 0, 0]),
            (3, -1, [0, 0, 0]),
        ],
    )
    def test_attempt_timeout_generator(self, request_t, operation_t, expected_list):
        """
        test different values for timeouts. Clock is incremented by 1 second for each item in expected_list
        """
        timestamp_start = 123
        with mock.patch("time.monotonic") as mock_monotonic:
            mock_monotonic.return_value = timestamp_start
            generator = _helpers._attempt_timeout_generator(request_t, operation_t)
            for val in expected_list:
                mock_monotonic.return_value += 1
                assert next(generator) == val

    @pytest.mark.parametrize(
        "request_t,operation_t,expected",
        [
            (1, 3.5, 1),
            (None, 3.5, 3.5),
            (10, 5, 5),
            (5, 10, 5),
            (3, 3, 3),
            (0, 3, 0),
            (3, 0, 0),
            (-1, 3, 0),
            (3, -1, 0),
        ],
    )
    def test_attempt_timeout_frozen_time(self, request_t, operation_t, expected):
        """test with time.monotonic frozen"""
        timestamp_start = 123
        with mock.patch("time.monotonic") as mock_monotonic:
            mock_monotonic.return_value = timestamp_start
            generator = _helpers._attempt_timeout_generator(request_t, operation_t)
            assert next(generator) == expected
            # value should not change without time.monotonic changing
            assert next(generator) == expected

    def test_attempt_timeout_w_sleeps(self):
        """use real sleep values to make sure it matches expectations"""
        from time import sleep
        operation_timeout = 1
        generator = _helpers._attempt_timeout_generator(None, operation_timeout)
        expected_value = operation_timeout
        sleep_time = 0.1
        for i in range(3):
            found_value = next(generator)
            assert abs(found_value - expected_value) < 0.001
            sleep(sleep_time)
            expected_value -= sleep_time

class TestConvertRetryDeadline:
    """
    Test _convert_retry_deadline wrapper
    """

    @pytest.mark.asyncio
    async def test_no_error(self):
        async def test_func():
            return 1

        wrapped = _helpers._convert_retry_deadline(test_func, 0.1)
        assert await wrapped() == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("timeout", [0.1, 2.0, 30.0])
    async def test_retry_error(self, timeout):
        from google.api_core.exceptions import RetryError, DeadlineExceeded

        async def test_func():
            raise RetryError("retry error", None)

        wrapped = _helpers._convert_retry_deadline(test_func, timeout)
        with pytest.raises(DeadlineExceeded) as e:
            await wrapped()
        assert e.value.__cause__ is None
        assert f"operation_timeout of {timeout}s exceeded" in str(e.value)

    @pytest.mark.asyncio
    async def test_with_retry_errors(self):
        from google.api_core.exceptions import RetryError, DeadlineExceeded

        timeout = 10.0

        async def test_func():
            raise RetryError("retry error", None)

        associated_errors = [RuntimeError("error1"), ZeroDivisionError("other")]
        wrapped = _helpers._convert_retry_deadline(
            test_func, timeout, associated_errors
        )
        with pytest.raises(DeadlineExceeded) as e:
            await wrapped()
        cause = e.value.__cause__
        assert isinstance(cause, bigtable_exceptions.RetryExceptionGroup)
        assert cause.exceptions == tuple(associated_errors)
        assert f"operation_timeout of {timeout}s exceeded" in str(e.value)
