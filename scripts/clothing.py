import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")

#%%
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
    # chrome_options.binary_location = os.path.abspath("chrome-linux64/chrome")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    service = Service()
    driver = webdriver.Chrome(service=service,options=chrome_options)
    return driver

#%%
# URLs to scrape


urls = ["https://lawrencepur.com/collections/men-fabric-shirting",
               "https://lawrencepur.com/collections/men-fabric-summer-classic",
               "https://lawrencepur.com/collections/men-fabric-tropical-classic", 
               "https://www.gulahmedshop.com/mens-clothes/unstitched/latha-collection",
               "https://www.gulahmedshop.com/unstitched-fabric/lawn-collection/summer-essential-collection",
               "https://www.gulahmedshop.com/women/ideas-pret/dupattas",
               "https://www.gulahmedshop.com/women/unstitch_product_type-2pc"
]

#%%


# Function to scrape data
def scrape(url):
    driver = load_driver1()
    time.sleep(1)
    driver.get(url)
    
    if 'gulahmedshop' in url:
        time.sleep(5)
        try:
            cross_button = driver.find_element(By.CLASS_NAME, "action-close")
            time.sleep(1)
            if cross_button:
                cross_button.click()
            time.sleep(1)
        except:
            pass
        
        items = driver.find_elements(By.CLASS_NAME, "product-item-link")
        #current_prices = driver.find_elements(By.CLASS_NAME, "special-price .price")
        # prices = driver.find_elements(By.CLASS_NAME, "old-price .price")

        price_boxes = driver.find_elements(By.CSS_SELECTOR, 'div.price-box[data-role="priceBox"]')

        prices = []
        for box in price_boxes:
            try:
                # Try to get the old price if it exists
                old_price = box.find_element(By.CSS_SELECTOR, '[data-price-type="oldPrice"]')
                prices.append(old_price)
            except:
                # If old price is not found, get the final price
                final_price = box.find_element(By.CSS_SELECTOR, '[data-price-type="finalPrice"]')
                prices.append(final_price)

        
        time.sleep(1)
    else:
        time.sleep(2)
        items = driver.find_elements(By.CLASS_NAME, 't4s-product-title')
        prices = driver.find_elements(By.CLASS_NAME, 't4s-product-price')
        

    # Prepare dataframe
    df = pd.DataFrame(columns=['item', 'price']) 
    for item, price in zip(items, prices):
        df.loc[len(df)] = {"item" : item.text , "price" : price.text}
        
    df['url'] = url
    time.sleep(1)
    driver.close()
    return df

#%%
start_time = time.time()    
dfs = []
for url in urls:
    df = scrape(url)
    dfs.append(df)
    
    
df_combined = pd.concat(dfs, ignore_index=True)

print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file)

 
# %%
