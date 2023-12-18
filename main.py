import os
from dotenv import load_dotenv
import pickle
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import mysql.connector
load_dotenv()

connection = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE")
)
cursor = connection.cursor()

model_folder = 'models'
cities = [
    'NewTaipeiCity',
    'TaipeiCity',
    'TaoyuanCity',
    'TaichungCity',
    'TainanCity',
    'KaohsiungCity',
    'YilanCounty',
    'HsinchuCounty',
    'MiaoliCounty',
    'ChanghuaCounty',
    'NantouCounty',
    'YunlinCounty',
    'ChiayiCounty',
    'PingtungCounty',
    'TaitungCounty',
    'HualienCounty',
    'KeelungCity',
    'HsinchuCity',
    'ChiayiCity',
    'PenghuCounty',
    'KinmenCounty',
    'LienchiangCounty'
]
chinese_name = {
    'NewTaipeiCity': "新北市",
    'TaipeiCity': "台北市",
    'TaoyuanCity': "桃園市",
    'TaichungCity': "台中市",
    'TainanCity': "台南市",
    'KaohsiungCity': "高雄市",
    'YilanCounty': "宜蘭縣",
    'HsinchuCounty': "新竹縣",
    'MiaoliCounty': "苗栗縣",
    'ChanghuaCounty': "彰化縣",
    'NantouCounty': "南投縣",
    'YunlinCounty': "雲林縣",
    'ChiayiCounty': "嘉義縣",
    'PingtungCounty': "屏東縣",
    'TaitungCounty': "台東縣",
    'HualienCounty': "花蓮縣",
    'KeelungCity': "基隆市",
    'HsinchuCity': "新竹市",
    'ChiayiCity': "嘉義市",
    'PenghuCounty': "澎湖縣",
    'KinmenCounty': "金門縣",
    'LienchiangCounty': "連江縣"
}

for location in cities:
    model_file_path = os.path.join(model_folder, f'sarima_model_{location}.pkl')

    # 加載 SARIMA 模型
    with open(model_file_path, 'rb') as model_file:
        sarima_result = pickle.load(model_file)

    new_data = []  # 新資料加在這裡

    # 預測
    forecast_periods = 15  # 預測月數 從2023-10開始
    forecast = sarima_result.get_forecast(steps=forecast_periods, exog=new_data)

    forecast_mean = forecast.predicted_mean
    forecast_conf_int = forecast.conf_int()

    print(f'Prediction for dataset {chinese_name[location]}:')
    year: int = 2023
    month: int = 10
    for forecast_value in forecast_mean:
        value = int(forecast_value)
        # print(f'{year}-{month:02d}: {value}')
        query_str = f"insert into carbonmap (year, month, city, amount) values ({year}, {month}, \"{chinese_name[location]}\", {value});"
        print("query = ", query_str)
        cursor.execute(query_str)
        connection.commit()
        row_count = cursor.rowcount
        print(f"inserted {row_count} rows")
        month += 1
        if month == 13:
            month = 1
            year += 1
    # print(forecast_mean)

'''
    output_folder_name = 'predict_2023-10~2024-12'

    output_folder_path = os.path.join(os.getcwd(), output_folder_name)
    if not os.path.exists(output_folder_path):
        os.mkdir(output_folder_path)

    forecast_df = pd.DataFrame({
        'Date': pd.date_range(start='2023-10-01', periods=forecast_periods, freq='M'),  # 從2023-10開始
        'Electricity_Demand_Prediction': forecast_mean
    })
    output_csv_path = os.path.join(output_folder_path, f'forecast_{location}.csv')

    forecast_df.to_csv(output_csv_path, index=False)

    print(f'Prediction for dataset {location} saved to {output_csv_path}')


'''
cursor.close()
connection.close()
