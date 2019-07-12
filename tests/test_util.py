import unittest
import datetime

from netzero.sources import util


class TestSourceUtils(unittest.TestCase):
    def test_time_intervals_basic(self):
        start_date = datetime.datetime(2019, 6, 27)
        end_date = datetime.datetime(2019, 7, 3)

        expected_intervals = [
            (datetime.datetime(2019, 6, 27), datetime.datetime(2019, 6, 28)),
            (datetime.datetime(2019, 6, 28), datetime.datetime(2019, 6, 29)),
            (datetime.datetime(2019, 6, 29), datetime.datetime(2019, 6, 30)),
            (datetime.datetime(2019, 6, 30), datetime.datetime(2019, 7, 1)),
            (datetime.datetime(2019, 7, 1), datetime.datetime(2019, 7, 2)),
            (datetime.datetime(2019, 7, 2), datetime.datetime(2019, 7, 3)),
        ]

        gen = util.time_intervals(start_date, end_date)

        for expected, actual in zip(expected_intervals, gen):
            self.assertEqual(expected[0], actual[0])
            self.assertEqual(expected[1], actual[1])

    def test_time_intervals_off_interval(self):
        start_date = datetime.datetime(2019, 6, 27)
        end_date = datetime.datetime(2019, 7, 3)

        expected_intervals = [
            (datetime.datetime(2019, 6, 27), datetime.datetime(2019, 7, 1)),
            (datetime.datetime(2019, 7, 1), datetime.datetime(2019, 7, 3))
        ]

        gen = util.time_intervals(start_date, end_date, days=4)

        for expected, actual in zip(expected_intervals, gen):
            self.assertEqual(expected[0], actual[0])
            self.assertEqual(expected[1], actual[1])

    def test_time_intervals_off_interval_one(self):
        start_date = datetime.datetime(2019, 7, 1)
        end_date = datetime.datetime(2019, 7, 3)

        expected_intervals = [(datetime.datetime(2019, 7, 1),
                               datetime.datetime(2019, 7, 3))]

        gen = util.time_intervals(start_date, end_date, days=17)

        for expected, actual in zip(expected_intervals, gen):
            self.assertEqual(expected[0], actual[0])
            self.assertEqual(expected[1], actual[1])

    def test_time_intervals_empty(self):
        start_date = datetime.datetime(2019, 7, 12)
        end_date = datetime.datetime(2019, 7, 12)

        gen = util.time_intervals(end_date, start_date)

        interval = next(gen)

        self.assertEqual(start_date, interval[0])
        self.assertEqual(end_date, interval[1])

        with self.assertRaises(StopIteration):
            next(gen)

    def test_time_intervals_backwards(self):
        start_date = datetime.datetime(2019, 6, 27)
        end_date = datetime.datetime(2019, 7, 3)

        gen = util.time_intervals(end_date, start_date)

        with self.assertRaises(StopIteration):
            next(gen)
