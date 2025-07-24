# %%
import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")

# Load Libraries
import pandas as pd
import numpy as np
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import sys
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

def create_csv_path(current_time: str, suffix = ".csv") -> str:
    current_date = datetime.strptime(current_time, "%Y-%m-%d_%H-%M").date().isoformat()
    current_script_name = Path(__file__).stem
    path = os.path.join(DATA_DIR, current_date)
    if os.path.exists(path):
        csv_path = os.path.join(path, current_script_name + suffix)
        return csv_path
    else:
        os.mkdir(path)
        csv_path = os.path.join(path, current_script_name + suffix)
        return csv_path

# %%
# Initialize the ChromeDriver
# def load_driver():  # Auto download Chrome Driver
#     # Set up ChromeDriver using webdriver_manager
#     chrome_options = Options()
#     chrome_options.add_argument("--start-maximized")
#     #chrome_options.add_argument("--headless")  # Run in headless mode
#     #chrome_options.add_argument("--no-sandbox")
#     service = ChromeService(executable_path=ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service,options=chrome_options)
#     return driver
def load_driver1(): # Manual download
    # Set the path where chrome driver is placed
    path = r"D:\\hassan\\SBP Work\\Digital CPI\\chromedriver-win64\\chromedriver.exe"
    # path = "chromedriver-linux64/chromedriver" 
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    # chrome_options.add_argument("--no-sandbox")
    # chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--start-maximized")
    # chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.binary_location = r"D:\\hassan\\SBP Work\\Digital CPI\\chrome-win64\\chrome.exe"
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    service = Service(executable_path = path)
    driver = webdriver.Chrome(service=service,options=chrome_options)
    return driver

# %%
def scraping(df_web_split):
    df = pd.DataFrame(columns = ["Date","Org","Dest","Time","Airline","Price"]  )   
    driver = load_driver1()
    for index,rows in df_web_split.iterrows():
        try:
            driver.get(rows["Add"])
            time.sleep(2)
            wait = WebDriverWait(driver, 30)
            x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div._3Zeoqtbn9jD1nk4cjLVwz6'))) 
            wait_element = driver.find_elements(By.CSS_SELECTOR, 'div._3ZvI4mQK9bHvQEoD_k_wlG')
            while wait_element:
                time.sleep(10)
                wait_element = driver.find_elements(By.CSS_SELECTOR, 'div._3ZvI4mQK9bHvQEoD_k_wlG')
    
            #Start time
            s_time = driver.find_elements(By.XPATH, '//*[@data-test="search-flight-card-start-time"]')
            price = driver.find_elements(By.CSS_SELECTOR, 'div._3Zeoqtbn9jD1nk4cjLVwz6')
            #Airline
            airline = driver.find_elements(By.CSS_SELECTOR, 'div._23rHKnVSUs7BGqu-V-Pz9s')
            for i in range(0,len(airline)):
                df.loc[len(df)] = {"Date" : rows["Date"] ,
                                   "Org" : rows["Org"] , 
                                   "Dest" : rows["Dest"] , 
                                   "Time" : s_time[i].text , 
                                   "Price" : price[i].text,
                                   "Airline" : airline[i].text.replace('\n',' ') 
                                   
                                  }
        except:
            print(f"No data for flights from {rows['Org']} to {rows['Dest']} on {rows['Date']}" )
            continue
    
        print(f"Done: Flights from {rows['Org']} to {rows['Dest']} on {rows['Date']}" )

    return df

# %%
start_time = time.time()

org_des = [
    ("KHI","ISB"),
    ("KHI","LHE"),
    ("LHE","KHI"),
    ("ISB","KHI")
]
main_add ="https://www.sastaticket.pk/air/search?cabinClass={%22code%22:%22Y%22,%22label%22:%22Economy%22}&legs[]={%22departureDate%22:%22"
sdate = (datetime.now() + timedelta(days=5)).strftime('%Y-%m-%d')
dates = pd.date_range(sdate,periods = 14 ,freq='d')
df_web_address = pd.DataFrame(columns = ["Date","Add","Org","Dest"])
for i  in range(0,len(dates)):
    for j in range(0,len(org_des)):
        org , des = org_des[j]
        df_web_address.loc[len(df_web_address)] = {"Add": main_add \
                                + str(dates[i].strftime('%Y-%m-%d')) + \
                                "%22,%22origin%22:%22" \
                                + str(org) + \
                                "%22,%22destination%22:%22" \
                                + str(des) + \
                                "%22}&routeType=ONEWAY&travelerCount={%22numAdult%22:1,%22numChild%22:0,%22numInfant%22:0}",\
                                "Date" : dates[i] , "Org" : org , "Dest" : des}

total_rows = len(df_web_address)
split1 = total_rows // 3
split2 = 2 * total_rows // 3

df_web_address1 = df_web_address.iloc[:split1]
df_web_address2 = df_web_address.iloc[split1:split2]
df_web_address3 = df_web_address.iloc[split2:]

# List of DataFrame parts to be processed
dataframe_split = [df_web_address1, df_web_address2, df_web_address3]

#Running code with 5 threads
with ThreadPoolExecutor(max_workers = 3) as p:
    results = list(p.map(scraping, dataframe_split))

df_combined = pd.concat(results, ignore_index=True)

print(f"{(time.time() - start_time):.2f} seconds") 

now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file)


# %%



