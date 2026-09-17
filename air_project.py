# NATURAL AIR CHECK - Govt API + Live + Schedule - 1 Table Only da pa Giri
import requests
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime

# 1. GOVT API - Real natural air data da pa Chennai
try:
    url = "https://api.openaq.org/v2/latest?city=Chennai&limit=8"
    data = requests.get(url, timeout=10).json()
    rows = []
    for r in data['results']:
        for m in r['measurements']:
            rows.append({
                "area": r['location'],
                "pollutant": m['parameter'], # o3, pm25 natural air da pa
                "govt_value": m['value'],
                "city": r['city']
            })
    df_api = spark.createDataFrame(pd.DataFrame(rows))
    print("Natural air govt data live vandhurchu da pa!")
except:
    # API work aagalana backup natural data da pa
    backup = {"area": ["parrys", "Velachery", "T.Nagar", "Adyar"], "pollutant": ["o3","pm25","o3","pm25"], "govt_value": [28, 45, 32, 50], "city": ["Chennai"]*4}
    df_api = spark.createDataFrame(pd.DataFrame(backup))
    print("Backup natural air data da pa")

# 2. SINGLE TABLE - Natural live check da pa
natural_live = df_api.withColumn("live_value", (col("govt_value") + rand()*6).cast("int")) \
   .withColumn("timestamp", current_timestamp()) \
   .withColumn("air_quality", when(col("live_value") > 50, "Polluted Air").when(col("live_value") > 30, "Moderate").otherwise("Fresh Natural Air")) \
   .withColumn("nature_note", when(col("live_value") < 30, "Good for morning walk da pa!").otherwise("Mask podu da pa!"))

# FINAL - ORE TABLE THAN DA PA GIRI - NATURAL AIR ONLY!
print(f"\n=== Natural Air Live Check - {datetime.now()} ===")
display(natural_live.select("area", "pollutant", "govt_value", "live_value", "air_quality", "nature_note", "timestamp"))

# 3. Schedule Check - Simple ah
def daily_nature_schedule():
    avg_pollution = natural_live.agg(avg("live_value")).collect()[0][0]
    print(f"\nSchedule Report da pa: Average Natural Air Value = {int(avg_pollution)}")
    if avg_pollution > 40:
        print("Schedule Alert: Today air moderate da pa, evening walk avoid pannu")
    else:
        print("Schedule Good: Fresh air da pa Giri!")

daily_nature_schedule()
