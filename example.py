from api import SP500Api

# A quick example
# Victor Marin Felip

# Create the api object:
api = SP500Api()

# You can specify the path to the database.ini file and the connection identifier:
api = SP500Api(db_info="aprawa", db_ini_path="data/mysql_config.ini")

# Usage is very easy. You specify what you want (which columns, what kind of resampling)
# and then you specify in which format you want it (CS, JSON or as a plot)

# Get CSV string of quarterly data for all columns:
CSV = api.get_quarterly().as_csv()

# Get JSON string of yearly data for columns sp500 and dividend. Date must be added
# if columns are specified. Columns must match database column names. There is a system
# that will find the datetime column by its type, so column order is not very importand.
JSON = api.get_yearly(columns=["date", "sp500", "dividend"]).as_json()

# Get monthly data for every column as objects (a lsit and a tuple of tuples as rows):
header, data = api.get_monthly().as_raw_data()

# Get a custom 3 year aggregation of the data for some columns:
JSON = api.get_custom(timeframe="3Y", columns=["date", "sp500", "dividend"]).as_json()


# All of these examples will add colname+"_pct_change" columns with the percentual
# difference between a row and the previous one.

# Plotting:
# The api uses plotly to generate interactive plots with a graphical UI
# There will be as many plots as non date-like colums are specified. The date-like
# column will act as the X axis. This will open a browser tab to generate the visualization.
# Single plots can be disabled by clicking on their names in the plot legend.

fig = api.get_quarterly(columns=["date", "sp500", "dividend"]).plot()
fig.show()

# You can avoid calling the show() method:
api.get_quarterly(columns=["date", "sp500", "dividend"]).plot(show=True)
