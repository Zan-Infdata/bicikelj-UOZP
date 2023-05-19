import pandas as pd

# kongresni urad ljubljana / CD / Tivoli / Stozice / GR / Cela LJ

class DataPreparation(object):

        
    filepath = ""
    data = None
    weather = None
    station_list = []
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






    def addHistoryData(self, timestamp):
        sfx = timestamp[9:]
        self.data = pd.merge_asof(self.data.sort_values(self.TIMESTAMP), self.data.sort_values(timestamp), left_on=timestamp, right_on=self.TIMESTAMP, direction='nearest', suffixes=('', sfx))


    def cleanUpDuplicates(self):
        self.data = self.data.drop([col for col in self.data.columns if col.endswith('ago_30min_ago')], axis=1)
        self.data = self.data.drop([col for col in self.data.columns if col.endswith('ago_60min_ago')], axis=1)
        self.data = self.data.drop([col for col in self.data.columns if col.endswith('ago_90min_ago')], axis=1)
        self.data = self.data.drop([col for col in self.data.columns if col.endswith('ago_120min_ago')], axis=1)

        self.data = self.data.loc[:,~self.data.columns.duplicated()]



    def init(self, filepath: str, filepath_d: str, filepath_w: str, isTraining: bool):
        self.filepath = filepath
        # read the data from csv
        self.data = pd.read_csv(filepath_d)
        self.weather = pd.read_csv(filepath_w)

        #print(self.data)


        # get station list
        self.station_list = self.data.columns[1:].tolist()


        self.data[self.TIMESTAMP] = pd.to_datetime(self.data[self.TIMESTAMP])
        self.weather[self.TIMESTAMP] = pd.to_datetime(self.weather[self.TIMESTAMP_W])

        
        if (isTraining):

            # add data about bikes 30, 60, 90, 120 min ago
            self.data[self.TIMESTAMP_30] = self.data[self.TIMESTAMP] - pd.Timedelta('30 min')
            self.data[self.TIMESTAMP_60] = self.data[self.TIMESTAMP] - pd.Timedelta('60 min')
            self.data[self.TIMESTAMP_90] = self.data[self.TIMESTAMP] - pd.Timedelta('90 min')
            self.data[self.TIMESTAMP_120] = self.data[self.TIMESTAMP] - pd.Timedelta('120 min')


            self.addHistoryData(self.TIMESTAMP_30)
            self.addHistoryData(self.TIMESTAMP_60)
            self.addHistoryData(self.TIMESTAMP_90)
            self.addHistoryData(self.TIMESTAMP_120)

            self.cleanUpDuplicates()


        # create custom features
        self.data[self.DATE] = self.data[self.TIMESTAMP].dt.date
        self.data[self.MONTH] = self.data[self.TIMESTAMP].dt.month
        self.data[self.WEEK] = self.data[self.TIMESTAMP].dt.isocalendar().week
        self.data[self.DOW] = self.data[self.TIMESTAMP].dt.dayofweek
        self.data[self.DOM] = self.data[self.TIMESTAMP].dt.day

        # merge weather data
        self.data = pd.merge_asof(self.data.sort_values(self.TIMESTAMP), self.weather.sort_values(self.TIMESTAMP), on=self.TIMESTAMP, direction='nearest')

        # rename all weather data
        self.data[self.TEMPERATURE] = self.data[self.TEMPERATURE_W]
        self.data[self.RAIN] = self.data [self.RAIN_W]
        self.data[self.WIND] = self.data [self.WIND_W]

        # remove unwanted columns
        self.data = self.data.drop(columns=[self.STATION_W,
                                  self.STATION_NAME_W,
                                  self.TIMESTAMP_W,
                                  self.TEMPERATURE_W,
                                  self.RAIN_W,
                                  self.WIND_W,
                                  self.TIMESTAMP])


    def toCsv(self):
        
        self.data.to_csv(self.filepath,index=False, date_format='%Y-%m-%d %H:%M:%S', encoding="utf-8", sep=";")



train_data = DataPreparation()
train_data.init("data/out.csv","data/bicikelj_train.csv","data/weather.csv", True)

