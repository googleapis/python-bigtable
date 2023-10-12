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
import google.cloud.bigtable.data._helpers as _helpers
import google.cloud.bigtable.data.exceptions as bigtable_exceptions

import mock


class TestMakeMetadata:
    @pytest.mark.parametrize(
        "table,profile,expected",
        [
            ("table", "profile", "table_name=table&app_profile_id=profile"),
            ("table", None, "table_name=table"),
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


class TestExponentialSleepGenerator:
    @mock.patch("random.uniform", autospec=True, side_effect=lambda m, n: m)
    @pytest.mark.parametrize(
        "args,expected",
        [
            ((), [0.01, 0.02, 0.03, 0.04, 0.05]),  # test defaults
            ((1, 3, 2, 1), [1, 2, 3, 3, 3]),  # test hitting limit
            ((1, 3, 2, 0.5), [1, 1.5, 2, 2.5, 3, 3]),  # test with smaller min_increase
            ((0.92, 3, 2, 0), [0.92, 0.92, 0.92]),  # test with min_increase of 0
            ((1, 3, 10, 0.5), [1, 1.5, 2, 2.5, 3, 3]),  # test with larger multiplier
            ((1, 25, 1.5, 5), [1, 6, 11, 16, 21, 25]),  # test with larger min increase
            ((1, 5, 1, 0), [1, 1, 1, 1]),  # test with multiplier of 1
            ((1, 5, 1, 1), [1, 2, 3, 4]),  # test with min_increase with multiplier of 1
        ],
    )
    def test_exponential_sleep_generator_lower_bound(self, uniform, args, expected):
        """
        Test that _exponential_sleep_generator generated expected values when random.uniform is mocked to return
        the lower bound of the range

        Each yield should consistently be min_increase above the last
        """
        import itertools

        gen = _helpers._exponential_sleep_generator(*args)
        result = list(itertools.islice(gen, len(expected)))
        assert result == expected

    @mock.patch("random.uniform", autospec=True, side_effect=lambda m, n: n)
    @pytest.mark.parametrize(
        "args,expected",
        [
            ((), [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28]),  # test defaults
            ((1, 3, 2, 1), [1, 2, 3, 3, 3]),  # test hitting limit
            ((1, 3, 2, 0.5), [1, 2, 3, 3]),  # test with smaller min_increase
            ((0.92, 3, 2, 0), [0.92, 1.84, 3, 3]),  # test with min_increase of 0
            ((1, 5000, 10, 0.5), [1, 10, 100, 1000]),  # test with larger multiplier
            ((1, 20, 1.5, 5), [1, 6, 11, 16.5, 20]),  # test with larger min increase
            ((1, 5, 1, 0), [1, 1, 1, 1]),  # test with multiplier of 1
            ((1, 5, 1, 1), [1, 2, 3, 4]),  # test with min_increase with multiplier of 1
        ],
    )
    def test_exponential_sleep_generator_upper_bound(self, uniform, args, expected):
        """
        Test that _exponential_sleep_generator generated expected values when random.uniform is mocked to return
        the upper bound of the range

        Each yield should be scaled by multiplier
        """
        import itertools

        gen = _helpers._exponential_sleep_generator(*args)
        result = list(itertools.islice(gen, len(expected)))
        assert result == expected

    @pytest.mark.parametrize(
        "kwargs,exc_msg",
        [
            ({"initial": 0}, "initial must be > 0"),
            ({"initial": -1}, "initial must be > 0"),
            ({"multiplier": 0}, "multiplier must be >= 1"),
            ({"multiplier": -1}, "multiplier must be >= 1"),
            ({"multiplier": 0.9}, "multiplier must be >= 1"),
            ({"min_increase": -1}, "min_increase must be >= 0"),
            ({"min_increase": -0.1}, "min_increase must be >= 0"),
            ({"initial": 1, "maximum": 0}, "maximum must be >= initial"),
            ({"initial": 2, "maximum": 1}, "maximum must be >= initial"),
            ({"initial": 2, "maximum": 1.99}, "maximum must be >= initial"),
        ],
    )
    def test_exponential_sleep_generator_bad_arguments(self, kwargs, exc_msg):
        """
        Test that _exponential_sleep_generator raises ValueError when given unexpected 0 or negative values
        """
        with pytest.raises(ValueError) as excinfo:
            gen = _helpers._exponential_sleep_generator(**kwargs)
            # start generator
            next(gen)
        assert exc_msg in str(excinfo.value)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"multiplier": 1},
            {"multiplier": 1.1},
            {"multiplier": 2},
            {"min_increase": 0},
            {"min_increase": 0.1},
            {"min_increase": 100},
            {"multiplier": 1, "min_increase": 0},
            {"multiplier": 1, "min_increase": 4},
        ],
    )
    def test_exponential_sleep_generator_always_increases(self, kwargs):
        """
        Generate a bunch of sleep values without random mocked, to ensure they always increase
        """
        gen = _helpers._exponential_sleep_generator(**kwargs, maximum=float("inf"))
        last = next(gen)
        for i in range(100):
            current = next(gen)
            assert current >= last
            last = current


class TestConvertRetryDeadline:
    """
    Test _convert_retry_deadline wrapper
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_async", [True, False])
    async def test_no_error(self, is_async):
        def test_func():
            return 1

        async def test_async():
            return test_func()

        func = test_async if is_async else test_func
        wrapped = _helpers._convert_retry_deadline(func, 0.1, is_async)
        result = await wrapped() if is_async else wrapped()
        assert result == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("timeout", [0.1, 2.0, 30.0])
    @pytest.mark.parametrize("is_async", [True, False])
    async def test_retry_error(self, timeout, is_async):
        from google.api_core.exceptions import RetryError, DeadlineExceeded

        def test_func():
            raise RetryError("retry error", None)

        async def test_async():
            return test_func()

        func = test_async if is_async else test_func
        wrapped = _helpers._convert_retry_deadline(func, timeout, is_async=is_async)
        with pytest.raises(DeadlineExceeded) as e:
            await wrapped() if is_async else wrapped()
        assert e.value.__cause__ is None
        assert f"operation_timeout of {timeout}s exceeded" in str(e.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_async", [True, False])
    async def test_with_retry_errors(self, is_async):
        from google.api_core.exceptions import RetryError, DeadlineExceeded

        timeout = 10.0

        def test_func():
            raise RetryError("retry error", None)

        async def test_async():
            return test_func()

        func = test_async if is_async else test_func

        associated_errors = [RuntimeError("error1"), ZeroDivisionError("other")]
        wrapped = _helpers._convert_retry_deadline(
            func, timeout, associated_errors, is_async
        )
        with pytest.raises(DeadlineExceeded) as e:
            await wrapped()
        cause = e.value.__cause__
        assert isinstance(cause, bigtable_exceptions.RetryExceptionGroup)
        assert cause.exceptions == tuple(associated_errors)
        assert f"operation_timeout of {timeout}s exceeded" in str(e.value)


class TestValidateTimeouts:
    def test_validate_timeouts_error_messages(self):
        with pytest.raises(ValueError) as e:
            _helpers._validate_timeouts(operation_timeout=1, attempt_timeout=-1)
        assert "attempt_timeout must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            _helpers._validate_timeouts(operation_timeout=-1, attempt_timeout=1)
        assert "operation_timeout must be greater than 0" in str(e.value)

    @pytest.mark.parametrize(
        "args,expected",
        [
            ([1, None, False], False),
            ([1, None, True], True),
            ([1, 1, False], True),
            ([1, 1, True], True),
            ([1, 1], True),
            ([1, None], False),
            ([2, 1], True),
            ([0, 1], False),
            ([1, 0], False),
            ([60, None], False),
            ([600, None], False),
            ([600, 600], True),
        ],
    )
    def test_validate_with_inputs(self, args, expected):
        """
        test whether an exception is thrown with different inputs
        """
        success = False
        try:
            _helpers._validate_timeouts(*args)
            success = True
        except ValueError:
            pass
        assert success == expected
