# %%
# Load Libraries
import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")

# https_proxy = os.environ.get("https_proxy")

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
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    # Only Linux
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    #######
    chrome_options.add_argument("--window-size=1920,1080")
    # chrome_options.add_argument("--start-maximized")
    # chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.binary_location = os.path.abspath("/usr/bin/chromium")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # service = Service()
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver', options=chrome_options)
    return driver

# %%



# %%
driver = load_driver1()

df = pd.DataFrame(columns = ["Item", "Area","Rate","Desc"])
try:
    driver.get("https://gold.pk/gold-rates-pakistan.php")
    wait = WebDriverWait(driver, 20)
    x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'p.goldratehome'))) 
    main_rate = driver.find_element(By.CSS_SELECTOR , "p.goldratehome")
    df.loc[len(df)] = {"Item" : "Gold" , "Area" : "Pakistan" , "Rate" : main_rate.text , "Desc" : "Pakistan 24 Karat Gold Rate per Tola"}
    city_rate = driver.find_elements(By.CSS_SELECTOR , "div.progress-table")[1].find_elements(By.CSS_SELECTOR , "div.table-row")
    for rate in city_rate:
        df.loc[len(df)] = {"Item" : "Gold" ,
                           "Area" : rate.find_element(By.XPATH, '(div)[3]').text ,
                           "Rate" : rate.find_element(By.XPATH, '(div)[4]').text ,
                           "Desc" : "24 Karat Gold Rate per Tola"}
    driver.get("https://gold.pk/pakistan-silver-rates-xagp.php")
    wait = WebDriverWait(driver, 20)
    x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'p.goldratehome'))) 
    main_rate = driver.find_element(By.CSS_SELECTOR , "p.goldratehome")
    df.loc[len(df)] = {"Item" : "Silver" , "Area" : "Pakistan" , "Rate" : main_rate.text , "Desc" : "Pakistan 24 Karat Silver Rate per Tola"}
    print("Gold and Silver rate done")
except:
    print("Retry")
    raise
driver.quit()
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df.to_csv(file)

# %%
df

