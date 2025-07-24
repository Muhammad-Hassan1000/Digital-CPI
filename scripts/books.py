#%% Load Libraries
import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")


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
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import re
import time
import sys
import random
from concurrent.futures import ThreadPoolExecutor
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

#%%
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

#%%

# driver = load_driver1()
# driver.get("https://liaqatbookdepot.com/shop")


urls = ['https://liaqatbookdepot.com/product-category/10-class/',
 'https://liaqatbookdepot.com/product-category/11-class/',
 'https://liaqatbookdepot.com/product-category/12-class/',
 'https://liaqatbookdepot.com/product-category/9-class/',
 'https://liaqatbookdepot.com/product-category/books/',
 'https://liaqatbookdepot.com/product-category/english/',
 'https://liaqatbookdepot.com/product-category/gernal-math/',
 'https://liaqatbookdepot.com/product-category/gernal-science/',
 'https://liaqatbookdepot.com/product-category/math/',
 'https://liaqatbookdepot.com/product-category/urdu/',
 "https://liaqatbookdepot.com/product-category/stationery/"

]

def scrape_items():
    df = pd.DataFrame(columns=["Item", "Price"])
    try:
        # Locate all the items on the page (Modify the selector as per your page structure)
        items = driver.find_elements(By.CLASS_NAME, 'product-title')
        prices = driver.find_elements(By.CLASS_NAME, 'product-price')

        # Extract and print (or store) item details
        for item, price in zip(items, prices):
            df.loc[len(df)] = {"Item": item.text, "Price": price.text}
        return df
    except Exception as e:
        print(f"Error fetching products: {e}")
        return df
#%%
start_time = time.time()
dfs = []
for url in urls:
    # url = urls[1]
    driver = load_driver1()
    driver.get(url)
    time.sleep(2)
    # dfs = []
    while True:
        # Scrape items from the current page
        df = scrape_items()
        df['url'] = url
        dfs.append(df)
        try:
            # Find the "Next" button and click it
            next_button = driver.find_element(By.CSS_SELECTOR, 'a.next.page-numbers')
            next_button.click()
            
            # Optionally, add a delay to ensure the page fully loads before scraping again
            time.sleep(2)  # You can adjust or use WebDriverWait for more precise control
            
        except NoSuchElementException as e:
            # If the "Next" button is not found, break the loop
            print(f'{e}')
            print("No more pages.")
            break

    driver.quit()

end_time = time.time()
print(f"Scraping completed in {end_time - start_time} seconds")


#%%

df_combined = pd.concat(dfs, axis = 0)
print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file)



# %%
# show_more = driver.find_element(By.LINK_TEXT, '+ Show more')
# show_more.click()

# elements = driver.find_elements(By.CSS_SELECTOR, 'li[class^="cat-item"] a')
# hrefs = [element.get_attribute('href') for element in elements]