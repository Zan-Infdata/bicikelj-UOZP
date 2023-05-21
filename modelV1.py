from sklearn.linear_model import LinearRegression
import pandas as pd

from dataPreparation_cls import DataPreparation

TRAIN_DATA_FILE = "data/bicikelj_train.csv"
TEST_DATA_FILE = "data/bicikelj_test.csv"
WEATHER_DATA_FILE = "data/weather.csv"
OUTPUT_DATA_FILE = "result/bicikelj_out"

SUFIX_30 = "_30min_ago"
SUFIX_60 = "_60min_ago"
SUFIX_90 = "_90min_ago"
SUFIX_120 = "_120min_ago"
SUFIX_MA = "_moving_average"



features = [
    DataPreparation.YEAR,
    DataPreparation.MONTH,
    DataPreparation.WEEK,
    DataPreparation.DOW,
    DataPreparation.DOM,
    DataPreparation.TEMPERATURE,
    DataPreparation.WIND,
    DataPreparation.RAIN,
    DataPreparation.HOUR,
    DataPreparation.MINUTE,
    DataPreparation.SCHOOL_DAY,
    DataPreparation.RUSH_HOUR
]

# init model
lr = LinearRegression()

train_data = DataPreparation()
train_data.init(out_file=OUTPUT_DATA_FILE, data_file=TRAIN_DATA_FILE, weather_file=WEATHER_DATA_FILE, test_file=TEST_DATA_FILE)


data_out = pd.DataFrame()
data_out[DataPreparation.TIMESTAMP] = train_data.test_timestams

stations = train_data.station_list

# train a model for each station
for station in stations:
    # get features for each station
    station_features = [
        station + SUFIX_30,
        station + SUFIX_60,
        station + SUFIX_90,
        station + SUFIX_120
    ]

    

    features.extend(station_features)

    print (features)

    # extract necessary data
    X = train_data.data[features]

    X_predict = train_data.test[features]

    


    lr.fit(X, train_data.data[station])
    
    y_predict = lr.predict(X_predict)

    data_out[station] = y_predict

    # remove station features for next station
    features = features[:len(features)-len(station_features)]

train_data.toCsv()

# round the data
data_out = data_out.round(0)
data_out.to_csv(OUTPUT_DATA_FILE+"_predict_rounded.csv",index=False, date_format='%Y-%m-%d %H:%M:%S', encoding="utf-8", sep=",")




