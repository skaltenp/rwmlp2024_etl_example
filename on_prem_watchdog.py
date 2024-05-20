import os
import sys
import logging
import time
import datetime
import json
import requests
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
from watchdog.observers import Observer
from sqlalchemy import create_engine
from watchdog.events import FileSystemEvent, FileSystemEventHandler


class OverrideHandler(FileSystemEventHandler):
    def extract(self, file_path):
        meter_data = None
        weather_data = pd.DataFrame(columns=["timestamp", "site_id", "air_temperature", "cloud_coverage", "dew_temperature", "precip_depth_1_hr", "sea_level_pressure", "wind_direction", "wind_speed", ])

        meter_data = pd.DataFrame(columns=["timestamp", "site_id", "building_id", "meter" ,"meter_reading"])
        
        data = pd.DataFrame(columns=["timestamp", "site_id", "building_id", "meter_id" ,"meter_reading"])
        if file_path.endswith(".csv") and "meter_reading" not in file_path:
            try:
                data = pd.read_csv(file_path)
                if len(data) > 0: 
                    data[["site_id", "building_id", "meter_id"]] = file_path.replace(".csv", "").split("dso_data/")[-1].split("/")
                    data = data[["timestamp", "site_id", "building_id", "meter_id", "meter_reading",]].copy()
                    data.timestamp = data.timestamp.astype(str)
                #display(data)
            except EmptyDataError as e:
                print(e)
        elif file_path.endswith(".csv") and "meter_reading" in file_path:
            try:
                data = pd.read_csv(file_path)
                if len(data) > 0: 
                    row = file_path.replace(".csv", "").split("dso_data/")[-1].split("/")
                    row.remove("meter_reading")
                    data[["site_id", "building_id"]] = row
                    #dfs = []
                    #for meter_id in ["meter_0", "meter_1", "meter_2", "meter_3", ]:
                    #    df = data[["timestamp", "site_id", "building_id", meter_id]].copy()
                    #    df.columns = ["timestamp", "site_id", "building_id", "meter_reading"]
                    #    df["meter_id"] = meter_id
                    #    dfs.append(df)
                    #data = pd.concat(dfs)
                    data = data.melt(
                        id_vars=["timestamp", "site_id", "building_id", ], 
                        var_name="meter_id", 
                        value_name="meter_reading",
                    )
                    data["meter_id"] = data["meter_id"].apply(lambda x: int(x.split("_")[-1]))
                    data = data[["timestamp", "site_id", "building_id", "meter_id", "meter_reading",]].copy()
                    data.timestamp = data.timestamp.astype(str)
                #display(data)
            except EmptyDataError as e:
                print(e)
        elif file_path.endswith(".json"):
            try:
                data = pd.read_json(file_path, orient="index")
                if len(data) > 0:    
                    data[["site_id", "building_id", "meter_id"]] = file_path.replace(".json", "").split("dso_data/")[-1].split("/")
                    data = data.reset_index()
                    data = data.rename(columns={"index": "timestamp"})
                    data = data[["timestamp", "site_id", "building_id", "meter_id", "meter_reading",]].copy()
                    data.timestamp = data.timestamp.astype(str)
                #display(data)
            except Exception as e:
                print(e)
        if len(data) > 0:
            meter_data = data
            #print("#" * 25)
            meter_data["meter_id"] = meter_data["meter_id"].astype(int)
            meter_data["building_id"] = meter_data["building_id"].astype(int)
            meter_data = meter_data.rename(columns={"meter_id": "meter"})
            weather_data_list = []
            for site_id in meter_data.site_id.unique():
                res_hist = requests.get("https://weatherfakeapi.de/forecast/", params={'site': site_id, 'day': meter_data.timestamp.min().split()[0]})
                content = json.loads(res_hist.content)["results"]
                weather = pd.DataFrame.from_dict(content)
                weather_data_list.append(weather)
            weather_data = pd.concat(weather_data_list)
            if len(weather_data) < 1:
                weather_data = pd.DataFrame(columns=["timestamp", "site_id", "air_temperature", "cloud_coverage", "dew_temperature", "precip_depth_1_hr", "sea_level_pressure", "wind_direction", "wind_speed", ])
        return meter_data, weather_data

    def transform_first(self, meter_data, weather_data):
        #display(meter_data)
        meter_data = meter_data[["timestamp", "building_id", "meter", "meter_reading"]].reset_index(drop=True).copy()
        return [["meter_data",  meter_data[meter_data["timestamp"] >= "2016-09-30 00:00:00"]], ["weather_data", weather_data[weather_data["timestamp"] >= "2016-09-30 00:00:00"]], ]
    
    def load(self, table_names_dfs):
        engine = create_engine(self.URL)
        for [table_name, df] in table_names_dfs:    
            with engine.begin() as connection:
                df.to_sql(name=table_name, con=connection, if_exists='append', index=False)

    def __init__(self):
        super(OverrideHandler, self).__init__()
        with open("dso_data.csv", "w") as f:
            f.write("timestamp,site_id,building_id,meter_id,meter_reading\n")
        USER = ""
        PASSWORD = ""
        self.URL = f"mysql+pymysql://{USER}:{PASSWORD}@localhost/etl?charset=utf8mb4"

    def on_modified(self, event):
        print(f"Path modified: {event.src_path}")
        meter_data, weather_data = self.extract(event.src_path)
        tables_dfs = self.transform_first(meter_data, weather_data)
        self.load(tables_dfs)

path = "dso_data"
event_handler = OverrideHandler()
observer = Observer()
observer.schedule(event_handler, path, recursive=True)
observer.start()
try:
    while observer.is_alive():
        observer.join(1)
finally:
    observer.stop()
    observer.join()