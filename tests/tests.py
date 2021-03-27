from api import Results, SP500Api
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from dateparser import parse
import json
import unittest


class ResultsTest(unittest.TestCase):

    def setUp(self):

        start = datetime(1990, 1, 1)
        end = datetime(2000, 1, 1)
        delta = [start + timedelta(days=x) for x in range((end-start).days + 1)]

        dataset = []
        k = 0
        for day in delta:
            dataset.append((day, k % 4 + 1, k % 7 + 1))
            k += 1

        self.values = tuple(dataset)
        self.header = ["date", "var_1", "var_2"]

    def testReturnsCSV(self):
        result = Results(self.header, self.values)
        csv = result.as_csv()
        # Testing that it returns a string
        self.assertIsInstance(csv, str)
        lines = csv.split("\n")[:-1]
        # testing that there are as many records as values + 1 header
        self.assertEqual(len(lines), len(self.values) + 1)
        # Testing that lines have as many columns as the header + 1 index column
        self.assertEqual(len(lines[0].split(",")), len(self.header) + 1)
        self.assertEqual(len(lines[238].split(",")), len(self.header) + 1)

    def testCSVHeaderInRightOrder(self):
        result = Results(self.header, self.values)
        csv = result.as_csv()
        header = csv.split("\n")[0]
        expected_header = ","+",".join(self.header)
        self.assertEqual(header, expected_header)

    def testReturnsJSON(self):
        result = Results(self.header, self.values)

        # Test that the return string can be parsed as a json
        try:
            json_str = json.loads(result.as_json())
        except JSONDecodeError:
            self.fail()

        # Testing that the header is the same as the json fields
        # TODO this may fail since dicts don't retain their order
        self.assertEqual(list(json_str[0].keys()), self.header)
        self.assertEqual(list(json_str[658].keys()), self.header)

        # Test the number of records
        self.assertEqual(len(json_str), len(self.values))

    def testResamples(self):
        result = Results(self.header, self.values, timeframe="Y")
        csv = result.as_csv()
        lines = csv.split("\n")[1:-1]
        some_date = parse(lines[7].split(",")[1])
        some_date_plus_a_year = parse(lines[8].split(",")[1])
        delta = some_date_plus_a_year - some_date
        # Test that the difference between rows equals one year
        self.assertEqual(delta, timedelta(days=365))

    def testNamesPCTColumns(self):
        result = Results(self.header, self.values, timeframe="Y")
        header, data = result.as_raw_data()
        expected_header = self.header + [x+"_pct_change" for x in self.header[1:]]
        # Test that when resampling we get extra columns with the right name
        self.assertEqual(list(header), expected_header)

    def testValuesPCTColumns(self):
        result = Results(self.header, self.values, timeframe="Y")
        header, data = result.as_raw_data()
        val_some_year = data[5][2]
        val_next_year = data[6][2]
        pct = data[6][4]
        expected_pct = (val_next_year / val_some_year) - 1
        self.assertEqual(pct, expected_pct)

    def testRaisesValueError(self):
        # If no datetime type column when resampling a ValueError should happen
        # We transpose values to discard the date and transpose it again to original format
        no_date_values = list(map(list, zip(*self.values)))[1:]
        no_date_values = tuple(map(list, zip(*no_date_values)))
        self.assertRaises(ValueError, Results, self.header[1:], no_date_values, timeframe="Y")


class ApiTest(unittest.TestCase):

    def setUp(self):
        self.api = SP500Api("aprawa", "../data/mysql_config.ini")

    def testReturnsResultObject(self):
        res1 = self.api.get_yearly()
        res2 = self.api.get_monthly()
        res3 = self.api.get_quarterly()
        res4 = self.api.get_custom()

        self.assertIsInstance(res1, Results)
        self.assertIsInstance(res2, Results)
        self.assertIsInstance(res3, Results)
        self.assertIsInstance(res4, Results)

    def testReturnsYearlyData(self):
        _, values = self.api.get_yearly().as_raw_data()
        some_year = parse(values[3][0])
        next_year = parse(values[4][0])
        delta = next_year - some_year
        self.assertEqual(delta, timedelta(days=365))

    def testReturnsMonthlyData(self):
        _, values = self.api.get_monthly().as_raw_data()
        some_month = parse(values[3][0])
        next_month = parse(values[4][0])
        delta = next_month - some_month
        self.assertEqual(delta, timedelta(days=31))

    def testReturnsQuarterlyData(self):
        _, values = self.api.get_quarterly().as_raw_data()
        some_quarter = parse(values[3][0])
        next_quarter = parse(values[4][0])
        delta = next_quarter - some_quarter
        self.assertEqual(delta, timedelta(days=91))

    def testSomeColumnsOnly(self):
        columns = ["date", "sp500", "dividend"]
        headers, _ = self.api.get_custom(columns=columns).as_raw_data()
        self.assertEqual(list(headers), ["date", "sp500", "dividend"])


if __name__ == '__main__':
    unittest.main()
