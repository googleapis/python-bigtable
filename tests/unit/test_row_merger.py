import unittest

class TestRowMerger(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _get_target_class():
        from gspread_asyncio.row_merger import RowMerger
        return RowMerger

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

class TestStateMachine(unittest.TestCase):

    @staticmethod
    def _get_target_class():
        from google.cloud.bigquery.row_merger import StateMachine
        return StateMachine

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)


class TestState(unittest.TestCase):
    pass

class TestRowBuilder(unittest.TestCase):

    @staticmethod
    def _get_target_class():
        from google.cloud.bigquery.row_merger import RowBuilder
        return RowBuilder

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)


