import sqlite3
import unittest
import unittest.mock
import datetime
import itertools

from netzero.sources import Weather

fake_results = {
    "results": [
        {
            "date": "2019-07-12T00:00:00",
            "value": 123,
            "station": "fake1"
        },
        {
            "date": "2019-07-12T00:00:00",
            "value": 456,
            "station": "fake2"
        },
    ]
}


class TestWeatherSource(unittest.TestCase):
    def setUp(self):
        self.config = {
            "weather": {
                "api_key": "fake",
                "stations": ["fake1", "fake2"]
            }
        }
        self.conn = sqlite3.connect(":memory:",
                                    detect_types=sqlite3.PARSE_DECLTYPES)

    def tearDown(self):
        self.conn.close()

    def test_weather_initialize(self):
        w = Weather(self.config, self.conn)

        expected_tables = [
            ("weather_day", ),
            ("weather_raw", ),
        ]

        actual_tables = self.conn.execute("""
            SELECT name
            FROM sqlite_master 
            WHERE 
                type ='table' AND 
                name NOT LIKE 'sqlite_%'
            ORDER BY name ASC;
        """)

        for expected, actual in zip(expected_tables, actual_tables):
            self.assertEqual(expected, actual)

    def test_weather_initialize_bad_config(self):
        with self.assertRaises(ValueError):
            Weather({"weather": {}}, self.conn)

        with self.assertRaises(ValueError):
            Weather({"weather": {"api_key": "fake"}}, self.conn)

        with self.assertRaises(ValueError):
            Weather({"weather": {"stations": ["fake1", "fake2"]}}, self.conn)

    @unittest.mock.patch.object(Weather,
                                "query_api",
                                return_value=fake_results)
    def test_weather_collect_small_interval(self, mock_query):
        start_date = datetime.date(2019, 7, 1)
        end_date = datetime.date(2019, 7, 3)

        w = Weather(self.config, self.conn)

        w.collect_data(start_date, end_date)

        mock_query.assert_called_once()

        call_args = mock_query.call_args[0]

        self.assertEqual(call_args[0], start_date)
        self.assertEqual(call_args[1], end_date)

    @unittest.mock.patch.object(Weather,
                                "query_api",
                                return_value=fake_results)
    def test_weather_collect_max_range(self, mock_query):
        # Three sources to get 1000 results in 333.33 days, which is less than
        # a year
        self.config["sources"] = ["fake1", "fake2", "fake3"]

        start_date = datetime.date(2016, 1, 1)
        end_date = start_date + datetime.timedelta(days=333)

        w = Weather(self.config, self.conn)

        w.collect_data(start_date, end_date)

        mock_query.assert_called_once()

        call_args = mock_query.call_args[0]

        self.assertEqual(call_args[0], start_date)
        self.assertEqual(call_args[1], end_date)

    @unittest.mock.patch.object(Weather,
                                "query_api",
                                return_value=fake_results)
    def test_weather_collect_multiple_intervals(self, mock_query):
        self.config["source"] = ["fake1"]

        start_date = datetime.date(2016, 1, 1)
        end_date = start_date + datetime.timedelta(
            days=365 * 2 + 365 / 2)  # 2.5 years

        w = Weather(self.config, self.conn)

        expected_intervals = [
            (start_date, start_date + datetime.timedelta(days=365)),
            (start_date + datetime.timedelta(days=365),
             start_date + datetime.timedelta(days=365 * 2)),
            (start_date + datetime.timedelta(days=365 * 2), end_date),
        ]

        w.collect_data(start_date, end_date)

        mock_query.assert_called()

        for expected_interval, call_args in itertools.zip_longest(
                expected_intervals, mock_query.call_args_list):
            args = call_args[0]

            self.assertEqual(expected_interval[0], args[0])
            self.assertEqual(expected_interval[1], args[1])

    @unittest.mock.patch.object(Weather,
                                "query_api",
                                return_value=fake_results)
    def test_weather_collect_same(self, mock_query):
        start_date = datetime.date(2019, 7, 6)
        end_date = start_date

        w = Weather(self.config, self.conn)

        w.collect_data(start_date, end_date)

        mock_query.assert_called_once()

        args = mock_query.call_args[0]

        self.assertEqual(start_date, args[0])
        self.assertEqual(end_date, args[1])

    @unittest.mock.patch.object(Weather,
                                "query_api",
                                return_value=fake_results)
    def test_weather_collect_backwards(self, mock_query):
        start_date = datetime.date(2019, 7, 6)
        end_date = datetime.date(2019, 7, 4)

        w = Weather(self.config, self.conn)

        w.collect_data(start_date, end_date)

        mock_query.assert_not_called()

    @unittest.mock.patch.object(Weather,
                                "query_api",
                                return_value=fake_results)
    def test_weather_collect_inserts(self, mock_query):
        start_date = datetime.date(2019, 1, 1)
        end_date = datetime.date(2019, 1, 1)

        w = Weather(self.config, self.conn)

        expected_rows = [
            (datetime.datetime(2019, 7, 12), 123, "fake1"),
            (datetime.datetime(2019, 7, 12), 456, "fake2"),
        ]

        w.collect_data(start_date, end_date)

        results = self.conn.execute("""
            SELECT time, value, station
            FROM weather_raw
            ORDER BY time, station
        """)

        for expected, actual in itertools.zip_longest(expected_rows, results):
            self.assertEqual(expected, actual)

    def test_weather_processing(self):
        w = Weather(self.config, self.conn)

        rows = [
            (datetime.datetime(2019, 7, 12), 2),
            (datetime.datetime(2019, 7, 12), 8),
            (datetime.datetime(2019, 7, 12), 5),
            (datetime.datetime(2019, 7, 11), 7),
            (datetime.datetime(2019, 7, 11), 3),
            (datetime.datetime(2019, 7, 10), 1),
            (datetime.datetime(2019, 7, 10), 4),
        ]

        self.conn.executemany(
            """
            INSERT INTO weather_raw(time, value) VALUES(?,?)
        """, rows)

        expected_rows = [
            (datetime.date(2019, 7, 12), 5),
            (datetime.date(2019, 7, 11), 5),
            (datetime.date(2019, 7, 10), 2.5),
        ]

        w.process_data()
        actual_rows = self.conn.execute("""
            SELECT *
            FROM weather_day
            ORDER BY date DESC
        """)

        for expected, actual in zip(expected_rows, actual_rows):
            self.assertEqual(expected[0], actual[0])
            self.assertEqual(expected[1], actual[1])
