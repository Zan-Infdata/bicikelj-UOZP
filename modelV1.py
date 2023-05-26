from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np
from progress.bar import PixelBar


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
    DataPreparation.TEMPERATURE,
    DataPreparation.WIND,
    DataPreparation.SCHOOL_DAY,
    DataPreparation.RUSH_HOUR,
    DataPreparation.WEEKEND,
]

features.extend(DataPreparation.IS_HOUR)
features.extend(DataPreparation.IS_DAY)

# init model
lr = LinearRegression()

# prepare data
train_data = DataPreparation()
train_data.init(out_file=OUTPUT_DATA_FILE, data_file=TRAIN_DATA_FILE, weather_file=WEATHER_DATA_FILE, test_file=TEST_DATA_FILE)

# seperate 1h and 2h prediction data
X_1_hour = train_data.test.iloc[::2]
X_2_hour = train_data.test.iloc[1::2]


data_out = pd.DataFrame()
data_out[DataPreparation.TIMESTAMP] = train_data.test_timestams

stations = train_data.station_list


bar = PixelBar("Training models and predicting:", max=len(stations))

# train a model for each station
for station in stations:


    # ------------------ 1h ago -----------------

    # get features for each station
    station_1h_features = [
        station + SUFIX_60,
        station + SUFIX_90,
        station + SUFIX_120
    ]



    features.extend(station_1h_features)


    # extract necessary data
    X = train_data.data[features]

    X_predict = X_1_hour[features]    


    lr.fit(X, train_data.data[station])
    
    y_1h_predict = lr.predict(X_predict)

    # remove station features for next station
    features = features[:len(features)-len(station_1h_features)]



    # ------------------ 2h ago -----------------

    # get features for each station
    station_2h_features = [
        station + SUFIX_120
    ]



    features.extend(station_2h_features)


    # extract necessary data
    X = train_data.data[features]

    X_predict = X_2_hour[features]    


    lr.fit(X, train_data.data[station])
    
    y_2h_predict = lr.predict(X_predict)

    # remove station features for next station
    features = features[:len(features)-len(station_2h_features)]
    

    y_predict = np.dstack((y_1h_predict,y_2h_predict)).flatten()

    data_out[station] = y_predict


    bar.next()


bar.finish()


# round the data
data_out = data_out.round(0)
data_out.to_csv(OUTPUT_DATA_FILE+"_predicted.csv",index=False, date_format='%Y-%m-%d %H:%M:%S', encoding="utf-8", sep=",")




