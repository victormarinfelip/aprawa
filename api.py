from db_connector import MySqlConnector
import pandas as pd
from typing import List, Tuple
import json
from datetime import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime
pd.options.plotting.backend = "plotly"


def check_data(fn):
    def wrapped(self=None):
        if len(self._header) == 0 or len(self._values) == 0:
            raise ValueError("No data retrieved from the database!")
        return fn(self)
    return wrapped


class Results(object):

    def __init__(self, header: List, values: Tuple, timeframe: str = None):
        """
        The Results object aggregates and processes a header list and value tuples
        containing rows of data. A timeframe can be specified to trigger the pandas
        resampling algorithm. All available pandas resample formats are available through
        the timeframe parameter. The class provides an easy way of organizing the data
        as CSV, JSON, or just return the raw data after resampling.

        If the resample is triggered for each non-datetime column a new one will be added
        with the percentual difference between one row and the previous one for that column.

        :param header: A list of strings with the header of the data.
        :param values: A tuple of data rows as tuples.
        :param timeframe: A string specifying the pandas resampling timeframe.
        """
        self._header = header
        self._values = values
        self._date_col_name = None
        if timeframe is not None:
            self._convert_data(timeframe)

    def as_raw_data(self) -> Tuple:
        """
        Returns the raw data, resampled if a timeframe was specified.

        :return: A tuple if (header, values)
        """
        return self._header, self._values

    @check_data
    def as_csv(self) -> str:
        """
        Returns a string in CSV format ready to be written into a file. An index column is added.

        :return: A string in CSV format of the whole dataset.
        """
        data = pd.DataFrame(self._values, columns=self._header)
        return data.to_csv(index=True, line_terminator='\n')

    @check_data
    def as_json(self):
        """
        Returns a string in JSON format with the data. Headers will be keys, and values will be values for these keys.

        :return: A string in JSON format of the whole dataset.
        """
        to_json = [[]]
        for row in self._values:
            to_json[-1] = {}
            for name, value in zip(self._header, row):
                if type(value) == datetime:
                    value = str(value)
                to_json[-1][name] = value
            to_json.append([])
        return json.dumps(to_json[:-1])

    def _convert_data(self, timeframe):
        """
        Resamples the dataset as per the timeframe argument and computes percentual
        returns.
        """
        data = pd.DataFrame(self._values, columns=self._header)
        data, self._date_col_name = self._use_date_as_index(data)

        resampled = data.resample(timeframe).apply(lambda x: x.iloc[-1])
        for column in data:
            series = resampled[column].pct_change(fill_method="ffill")
            resampled[column+"_pct_change"] = series

        resampled = resampled.reset_index()
        resampled[self._date_col_name] = resampled[self._date_col_name].astype(str)
        resampled = resampled.where(pd.notnull(resampled), None)
        self._values = resampled.values.tolist()
        self._header = resampled.columns.values

    @staticmethod
    def _use_date_as_index(data):
        for column in data:
            if is_datetime(data[column]):
                data = data.set_index(column)
                return data, column
        raise ValueError("No datetime-like column found in:\n{}".format(data))

    def plot(self, show=False):
        """
        Plots the dataset using plotly.

        :param show: Figure will be plotted (opens browser window).
        :return: A plotly figure.
        """
        data = pd.DataFrame(self._values, columns=self._header)
        data = data.set_index(self._date_col_name)
        fig = data.plot.bar()
        if show:
            fig.show()
        return fig


class SP500Api(object):

    def __init__(self, db_info: str = "aprawa", db_ini_path: str = "data/mysql_config.ini"):
        """
        A simple API to retrieve SP500 data. For every call to a get method a mysql connection will
        be started, a query will be executed, and the results processed.
        All get methods return a Results object with the result of the query passed.

        :param db_info: Database .ini identifyer
        :param db_ini_path: Mysql config.ini path
        """
        self.db_info = db_info
        self.db_ini_path = db_ini_path

    def get_custom(self, timeframe: str = None, columns: List[str] = None) -> Results:
        """
        Specify which columns to retrieve and which timeframe to resample to. A
        SQL query will be attempted against the provided database.

        A timeframe can be specified to trigger the resampling of the data into monthly, yearly,
        quarterly aggregates (or any other pandas supported aggregation). New columns will be added
        computing the percent relative difference between one period and the previous one. If no
        timeframe is provided then data will be presented as is with no resampling and no new columns.

        Specific columns can be retrieved instead of the whole dataset if specified.

        :param timeframe: The resampling timeframe, if any.
        :param columns: A list of column names in string format.
        :return: A Results object with the retrieved data passed into it.
        """
        conn = MySqlConnector(self.db_info, self.db_ini_path)
        sql = "SELECT {} FROM SP500"
        if columns is not None:
            cols = ",".join(columns)
            sql = sql.format(cols)
        else:
            sql = sql.format("*")

        header, values = conn.query(sql)
        conn.close()
        return Results(header, values, timeframe)

    def get_yearly(self, columns: List[str] = None) -> Results:
        """
        Returns a Results object with the data resampled into yearly aggregations. New columns
        will be added representing the percent difference between a row and the previous one, for each column.

        Specific columns can be retrieved instead of the whole dataset if specified

        :param columns: A list of column names in string format.
        :return: A Results object with the retrieved data passed into it.
        """
        return self.get_custom(timeframe="Y", columns=columns)

    def get_quarterly(self, columns: List[str] = None) -> Results:
        """
        Returns a Results object with the data resampled into quarterly aggregations. New columns
        will be added representing the percent difference between a row and the previous one, for each column.

        Specific columns can be retrieved instead of the whole dataset if specified

        :param columns: A list of column names in string format.
        :return: A Results object with the retrieved data passed into it.
        """
        return self.get_custom(timeframe="Q", columns=columns)

    def get_monthly(self, columns: List[str] = None) -> Results:
        """
        Returns a Results object with the data resampled into monthly aggregations. New columns
        will be added representing the percent difference between a row and the previous one, for each column.

        Specific columns can be retrieved instead of the whole dataset if specified

        :param columns: A list of column names in string format.
        :return: A Results object with the retrieved data passed into it.
        """
        return self.get_custom(timeframe="M", columns=columns)
