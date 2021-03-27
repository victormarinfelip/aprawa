import pandas as pd
from sqlalchemy.types import DateTime, Float
from db_connector import MySqlConnector

# First we define the mapping between csv header and database column names
csv_to_db_map = {
    "Date": "date",
    "SP500": "sp500",
    "Dividend": "dividend",
    "Earnings": "earnings",
    "Consumer Price Index": "consumer_price",
    "Long Interest Rate": "long_int_rate",
    "Real Price": "real_price",
    "Real Dividend": "real_dividend",
    "Real Earnings": "real_earnings",
    "PE10": "PE10"
}

# We also specify the dtypes
dtypes = {
    "date": DateTime,
    "sp500": Float,
    "dividend": Float,
    "earnings": Float,
    "consumer_price": Float,
    "long_int_rate": Float,
    "real_price": Float,
    "real_dividend": Float,
    "real_earnings": Float,
    "PE10": Float
}

# Lastly we use the very handy .to_sql() pandas function, using our SQLAlchemy engine created in our connector class
data = pd.read_csv("data/data_raw.csv")
data = data.rename(columns=csv_to_db_map)
connector = MySqlConnector("aprawa")
engine = connector.engine
data.to_sql("SP500", engine, if_exists='replace', index=False, chunksize=500, dtype=dtypes)
