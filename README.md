
## Extract and analyse South Australia's rents data


- Source - [South Australia Government Data Directory - Private Rent datasets](https://data.sa.gov.au/data/dataset/private-rent-report)

### Extract data 

- `python ./etl/download_rents_data.py`

### Transform and Load data

- `python ./etl/transform_rents_data.py`


### Analyse data

- See the Jupyter notebook `./analyse_sa_rents_data.ipynb`

- 1 Bedroom flat median price trend for Adelaide City
![Adelaide 1 Bedroom Flat Median Price Trend](images/adelaide_1br_flat_med_price_trend.png)
