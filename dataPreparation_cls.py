import pandas as pd
from progress.bar import PixelBar


# 0 - ni padavin,  - 1 - prši, - 10 -rahle padavine, - 30 -padavine, 30 -> ... - močnepadavine  

# kongresni urad ljubljana / CD / Tivoli / Stozice / GR / Cela LJ

class DataPreparation(object):

        
    filepath = ""
    data = None
    test = None
    test_timestams = None
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


    YEAR = "year"
    MONTH = "month"
    WEEK = "week"
    DOW = "day_of_week"
    DOM = "day_of_month"
    TEMPERATURE = "temperature"
    WIND = "wind"
    RAIN = "rain"
    HOUR = "hour"
    MINUTE = "minute"
    RUSH_HOUR = "is_rush_hour"
    SCHOOL_DAY = "is_school_day"
    WEEKEND = "is_weekend"
    TOD = "time_of_day"
    HOLIDAY = "is_holiday"

    IS_HOUR = [
        "is_1",
        "is_2",
        "is_3",
        "is_4",
        "is_5",
        "is_6",
        "is_7",
        "is_8",
        "is_9",
        "is_10",
        "is_11",
        "is_12",
        "is_13",
        "is_14",
        "is_15",
        "is_16",
        "is_17",
        "is_18",
        "is_19",
        "is_20",
        "is_21",
        "is_22",
        "is_23",
        "is_24"
    ]


    IS_DAY = [
        "is_monday",
        "is_tuesday",
        "is_wednesday",
        "is_thursday",
        "is_friday",
        "is_saturday",
        "is_sunday",
    ]



    HOLIDAYS = [
        '2022-08-15'
    ]
            


    def transformHourData(self):

        hr = 0
        for hour in self.IS_HOUR:
            hr += 1
            self.data[hour] = self.data[self.HOUR].apply(lambda x: 1 if x == hr else 0)
            self.test[hour] = self.test[self.HOUR].apply(lambda x: 1 if x == hr else 0)    



    def transformDayData(self):
        dy = 0
        for day in self.IS_DAY:
            self.data[day] = self.data[self.DOW].apply(lambda x: 1 if x == dy else 0)
            self.test[day] = self.test[self.DOW].apply(lambda x: 1 if x == dy else 0)
            dy += 1



    def addHistoryData(self, timestamp):
        sfx = timestamp[9:]
        self.test = pd.merge_asof(self.test.sort_values(self.TIMESTAMP), self.data.sort_values(timestamp), left_on=timestamp, right_on=self.TIMESTAMP, direction='nearest', suffixes=('', sfx))

        self.data = pd.merge_asof(self.data.sort_values(self.TIMESTAMP), self.data.sort_values(timestamp), left_on=timestamp, right_on=self.TIMESTAMP, direction='nearest', suffixes=('', sfx))

        


    def cleanUpDuplicates(self):
        self.data = self.data.drop([col for col in self.data.columns if col.endswith('ago_60min_ago')], axis=1)
        self.data = self.data.drop([col for col in self.data.columns if col.endswith('ago_90min_ago')], axis=1)
        self.data = self.data.drop([col for col in self.data.columns if col.endswith('ago_120min_ago')], axis=1)

        self.data = self.data.loc[:,~self.data.columns.duplicated()]

        self.test = self.test.drop([col for col in self.test.columns if col.endswith('ago_60min_ago')], axis=1)
        self.test = self.test.drop([col for col in self.test.columns if col.endswith('ago_90min_ago')], axis=1)
        self.test = self.test.drop([col for col in self.test.columns if col.endswith('ago_120min_ago')], axis=1)

        self.test = self.test.loc[:,~self.test.columns.duplicated()]



    def init(self, out_file: str, data_file: str, weather_file: str, test_file:str):
        self.filepath = out_file

        # read the data from csv
        self.data = pd.read_csv(data_file)
        self.weather = pd.read_csv(weather_file)
        self.test = pd.read_csv(test_file)

        # save test timestamps for later use
        self.test_timestams = self.test[self.TIMESTAMP]

        # get station list
        self.station_list = self.data.columns[1:].tolist()


        self.data[self.TIMESTAMP] = pd.to_datetime(self.data[self.TIMESTAMP])
        self.test[self.TIMESTAMP] = pd.to_datetime(self.test[self.TIMESTAMP])
        self.weather[self.TIMESTAMP] = pd.to_datetime(self.weather[self.TIMESTAMP_W])




        # add data about bikes 60, 90, 120 min ago
        self.data[self.TIMESTAMP_60] = self.data[self.TIMESTAMP] - pd.Timedelta('60 min')
        self.data[self.TIMESTAMP_90] = self.data[self.TIMESTAMP] - pd.Timedelta('90 min')
        self.data[self.TIMESTAMP_120] = self.data[self.TIMESTAMP] - pd.Timedelta('120 min')


        self.test[self.TIMESTAMP_60] = self.test[self.TIMESTAMP] - pd.Timedelta('60 min')
        self.test[self.TIMESTAMP_90] = self.test[self.TIMESTAMP] - pd.Timedelta('90 min')
        self.test[self.TIMESTAMP_120] = self.test[self.TIMESTAMP] - pd.Timedelta('120 min')


        self.addHistoryData(self.TIMESTAMP_60)
        self.addHistoryData(self.TIMESTAMP_90)
        self.addHistoryData(self.TIMESTAMP_120)

        self.cleanUpDuplicates()


        # create custom features data
        self.data[self.DOW] = self.data[self.TIMESTAMP].dt.dayofweek
        self.data[self.HOUR] = self.data[self.TIMESTAMP].dt.hour
        self.data[self.HOLIDAY] = self.data[self.TIMESTAMP].dt.date.apply(lambda x: 1 if str(x) in self.HOLIDAYS or x.month == 8 else 0)
        self.data[self.WEEKEND] = self.data[self.DOW].apply(lambda x: 1 if x > 4 else 0)


        # create custom features test
        self.test[self.DOW] = self.test[self.TIMESTAMP].dt.dayofweek
        self.test[self.HOUR] = self.test[self.TIMESTAMP].dt.hour
        self.test[self.HOLIDAY] = self.test[self.TIMESTAMP].dt.date.apply(lambda x: 1 if str(x) in self.HOLIDAYS or x.month == 8 else 0)
        self.test[self.WEEKEND] = self.test[self.DOW].apply(lambda x: 1 if x > 4 else 0)

  

        self.transformDayData()
        self.transformHourData()


        # merge weather data
        self.data = pd.merge_asof(left=self.data.sort_values(self.TIMESTAMP), right=self.weather.sort_values(self.TIMESTAMP), on=self.TIMESTAMP, direction='nearest')

        self.test = pd.merge_asof(left=self.test.sort_values(self.TIMESTAMP), right=self.weather.sort_values(self.TIMESTAMP), on=self.TIMESTAMP, direction='nearest')

        

        # rename all weather data
        self.data[self.TEMPERATURE] = self.data[self.TEMPERATURE_W]
        self.data[self.RAIN] = self.data [self.RAIN_W].apply(lambda x: 0 if x != x else x)

        self.test[self.TEMPERATURE] = self.test[self.TEMPERATURE_W]
        self.test[self.RAIN] = self.test [self.RAIN_W].apply(lambda x: 0 if x != x else x)


        # remove unwanted columns
        self.data = self.data.drop(columns=[self.STATION_W,
                                  self.STATION_NAME_W,
                                  self.TIMESTAMP_W,
                                  self.TEMPERATURE_W,
                                  self.RAIN_W,
                                  self.WIND_W,
                                  self.TIMESTAMP_60,
                                  self.TIMESTAMP_90,
                                  self.TIMESTAMP_120,
                                  ])
        
        
        self.test = self.test.drop(columns=[self.STATION_W,
                                  self.STATION_NAME_W,
                                  self.TIMESTAMP_W,
                                  self.TEMPERATURE_W,
                                  self.RAIN_W,
                                  self.WIND_W,
                                  self.TIMESTAMP_60,
                                  self.TIMESTAMP_90,
                                  self.TIMESTAMP_120,
                                  ])



        # remove starting data
        self.data = self.data.tail(-24)

        # bar init
        bar = PixelBar("Preparing data:                ", max=7258)

        inx = 1
        # remove unusable data
        while inx < len(self.data):
            if self.data.iloc[inx][self.TIMESTAMP] - self.data.iloc[inx-1][self.TIMESTAMP] > pd.Timedelta('2 hour'):
                self.data = self.data.drop(self.data.index[inx:inx+24])
            inx += 1
            bar.next()
        
        bar.finish()



    def toCsv(self):
        
        self.data.to_csv(self.filepath+"_data.csv",index=False, date_format='%Y-%m-%d %H:%M:%S', encoding="utf-8", sep=";")
        self.test.to_csv(self.filepath+"_test.csv",index=False, date_format='%Y-%m-%d %H:%M:%S', encoding="utf-8", sep=";")


