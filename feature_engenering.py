import pandas as pd
import numpy as np

# kongresni urad ljubljana / CD / Tivoli / Stozice / GR / Cela LJ


    
# constants
TIMESTAMP = "timestamp"
TIMESTAMP_30 = "timestamp_30min_ago"
TIMESTAMP_60 = "timestamp_60min_ago"
TIMESTAMP_90 = "timestamp_90min_ago"
TIMESTAMP_120 = "timestamp_120min_ago"

TIMESTAMP_W = " valid"
STATION_W = "station id"
STATION_NAME_W = " station name"
TEMPERATURE_W = "povp. T [°C]"
RAIN_W = "količina padavin [mm]"
WIND_W = "hitrost vetra [m/s]"


DATE = "date"
MONTH = "month"
WEEK = "week"
DOW = "day_of_week"
DOM = "day_of_month"
TEMPERATURE = "temperature"
WIND = "wind"
RAIN = "rain"
TIME = "time"






def addHistoryData(timestamp, data):
    sfx = timestamp[9:]
    return pd.merge_asof(data.sort_values(TIMESTAMP), data.sort_values(timestamp), left_on=timestamp, right_on=TIMESTAMP, direction='nearest', suffixes=('', sfx))


def cleanUpDuplicates(data):
    data = data.drop([col for col in data.columns if col.endswith('ago_30min_ago')], axis=1)
    data = data.drop([col for col in data.columns if col.endswith('ago_60min_ago')], axis=1)
    data = data.drop([col for col in data.columns if col.endswith('ago_90min_ago')], axis=1)
    data = data.drop([col for col in data.columns if col.endswith('ago_120min_ago')], axis=1)

    data = data.loc[:,~data.columns.duplicated()]

    return data

# read the data from csv
data = pd.read_csv("data/bicikelj_train.csv")
weather = pd.read_csv("data/weather.csv")



# get station list
station_list = data.columns[1:].tolist()


data[TIMESTAMP] = pd.to_datetime(data[TIMESTAMP])
weather[TIMESTAMP] = pd.to_datetime(weather[TIMESTAMP_W])


# add data about bikes 30, 60, 90, 120 min ago
data[TIMESTAMP_30] = data[TIMESTAMP] - pd.Timedelta('30 min')
data[TIMESTAMP_60] = data[TIMESTAMP] - pd.Timedelta('60 min')
data[TIMESTAMP_90] = data[TIMESTAMP] - pd.Timedelta('90 min')
data[TIMESTAMP_120] = data[TIMESTAMP] - pd.Timedelta('120 min')




data = addHistoryData(TIMESTAMP_30, data)
data = addHistoryData(TIMESTAMP_60, data)
data = addHistoryData(TIMESTAMP_90, data)
data = addHistoryData(TIMESTAMP_120, data)

data = cleanUpDuplicates(data)

# create custom features
data[DATE] = data[TIMESTAMP].dt.date
data[MONTH] = data[TIMESTAMP].dt.month
data[WEEK] = data[TIMESTAMP].dt.isocalendar().week
data[DOW] = data[TIMESTAMP].dt.dayofweek
data[DOM] = data[TIMESTAMP].dt.day
data[TIME] = data[TIMESTAMP].dt.time


# merge weather data
data = pd.merge_asof(data.sort_values(TIMESTAMP), weather.sort_values(TIMESTAMP), on=TIMESTAMP, direction='nearest')

# rename all weather data
data[TEMPERATURE] = data[TEMPERATURE_W]
data[RAIN] = data [RAIN_W]
data[WIND] = data [WIND_W]

# remove unwanted columns
data = data.drop(columns=[STATION_W, STATION_NAME_W, TIMESTAMP_W, TEMPERATURE_W, RAIN_W, WIND_W, TIMESTAMP])

# 0 - ni padavin,  - 1 - prši, - 10 -rahle padavine, - 30 -padavine, 30 -> ... - močnepadavine  
test = data[RAIN].apply(lambda x: 4 if x > 30
                             else 3 if x > 10
                             else 2 if x > 1
                             else 1 if x > 0.1
                             else 0)



