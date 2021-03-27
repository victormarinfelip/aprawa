# Aprawa Challenge
### Víctor Marín Felip

There is a full example in ```example.py```, but here is a quick look:

```{python3}
api = SP500Api()
CSV = api.get_quarterly().as_csv()
JSON = api.get_custom(timeframe="3Y", columns=["date", "sp500", "dividend"]).as_json()
api.get_quarterly(columns=["date", "sp500", "dividend"]).plot(show=True)
```

Basically ```result = <what do you want>.<in which format do you want it>``` is the philosophy 
here.

Right now ```data/mysql_config.ini``` points to my own database in a Vultr droplet. There is 
nothing else there, so feel free to do as you wish. To drop and rebuild the table
just run ```upload_dataset.py```. 

Thank you!
