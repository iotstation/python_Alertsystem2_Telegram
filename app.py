import os
import requests
import pandas as pd # used here to read the database response easily
import time
from io import StringIO # With StringIO: You create a "virtual file" instantly in memory, otherwise You would have to save resp.text to a real file (e.g., temp.csv), tell Pandas to open that file, read it
from dotenv import load_dotenv

# Load credentials from your .env
load_dotenv()

INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_URL = "https:........influxdata.com/api/v2/query"
INFLUX_ORG = "weather.parameter"
INFLUX_BUCKET = "green_house"  # Make sure this matches your ESP32 bucket!

TELE_TOKEN = os.getenv("TELE_TOKEN")
TELE_CHAT_ID = os.getenv("TELE_CHAT_ID")
TEMP_THRESHOLD = float(os.getenv("TEMP_THRESHOLD", 23.0))

def get_latest_temp():
    # Using your exact working Flux query structure
    flux_query = f'''
    from(bucket:"{INFLUX_BUCKET}")
      |> range(start: -10m)   
      |> filter(fn: (r) => r._measurement == "environment")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")   
    '''
    
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "application/vnd.flux; charset=utf-8"
    }

    try:
        resp = requests.post(INFLUX_URL, headers=headers, data=flux_query) # Sends the query to the cloud.
        if resp.status_code == 200 and resp.text.strip():
            # Parse the CSV response using your working pandas logic
            df = pd.read_csv(StringIO(resp.text))  # Pandas converts this CSV text into a clean data frame (table).
            if not df.empty and 'temperature' in df.columns:  # make sure the value is not empty and temperature column is exist
                return float(df['temperature'].iloc[-1])   #Float: to make sure Dec Number /This Filter just the "temperature" column from the table& iloc[-1] means last row 
        else:
            print(f"Query failed: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Error: {e}")
    return None

def send_telegram_alert(temp):
    message = f"🌱 Greenhouse Alert!\nCurrent Temp: {temp}°C\nThreshold: {TEMP_THRESHOLD}°C\nCheck your plants!"
    url = f"https://api.telegram.org/bot{TELE_TOKEN}/sendMessage?chat_id={TELE_CHAT_ID}&text={message}"
    requests.get(url)

if __name__ == "__main__":
    print("🚀 Alert System Active using working InfluxDB Connection...")
    while True:
        temp = get_latest_temp()
        if temp is not None:
            print(f"Latest Temperature: {temp}°C")
            if temp > TEMP_THRESHOLD:
                print("⚠️ Threshold exceeded! Sending Telegram...")
                send_telegram_alert(temp)
        else:
            print("Searching for data...")
            





#########################Content of .env files##################
#INFLUX_TOKEN= yout API Token
#INFLUX_ORG=your org name
#INFLUX_URL=https:..your cloud ip address....influxdata.com/api/v2/write?org=yout org&bucket=yout bucket&precision=s
#INFLUX_BUCKET=your bucket name

# Telegram Credentials
#TELE_TOKEN= yout telegram token
#TELE_CHAT_ID= your chat ID in telegram bot

# Threshold Settings
#TEMP_THRESHOLD=23.0