# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:43:04 2024

@author:
"""

# %%
# Load Libraries
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

def get_products(driver):
    # Create a DataFrame with two columns: Item and Price
    df = pd.DataFrame(columns=["Item", "Price"])
    
    try:
        # Wait until product card wrappers are located
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "card__information")))
        
        # Find the product titles and price boxes

        product_wrappers = driver.find_elements(By.CLASS_NAME, "card__information")

        # Loop through the product wrappers to get items and prices
        for wrapper in product_wrappers:
            try:
                # Extract product title
                item = wrapper.find_element(By.CLASS_NAME, "full-unstyled-link").text
                # Initialize price as "Price Not Available"
                price = "Price Not Available"
                
                # Look for any element containing "price-item" and extract the price
                price_elements = wrapper.find_elements(By.CLASS_NAME, "price-item")
                regular_price=None
                sale_price=None
                for price_element in price_elements:
                    # Check for full class names
                    if "price-item price-item--regular" in price_element.get_attribute("class"):
                        regular_price = price_element.text.strip()  # Set regular price if available
                    elif "price-item price-item--sale price-item--last" in price_element.get_attribute("class"):
                        sale_price = price_element.text.strip()  # Set sale price if no regular price

                # Assign price based on availability of regular or sale price
                price = regular_price if regular_price else sale_price if sale_price else price


                # Only add to DataFrame if item name and price are not empty
                if item and price:
                    df.loc[len(df)] = {"Item": item, "Price": price}
            
            except Exception as e:
                print(f"Error fetching item or price: {e}")
        
        return df
    
    except Exception as e:
        print(f"Error fetching products: {e}")
        return df
#%% scraping function

def scraping(url):
    driver = load_driver1()
    driver.get(url)
    sleeptime1 = 10
    sleeptime2 = 0.5
    time.sleep(sleeptime1)  # Wait for the page to load
    driver.execute_script("window.scrollBy(0, 500);")
    time.sleep(10)
    try:
        # Scroll until the page stops scrolling
        while True:
            old_scroll = driver.execute_script("return window.pageYOffset;")
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(sleeptime2)
            new_scroll = driver.execute_script("return window.pageYOffset;")

            if new_scroll == old_scroll:
                break

        df = get_products(driver)
        df['url'] = url
    except Exception as e:
        print(f"Cannot scrape due to {e}")
        df = pd.DataFrame(columns=['url', 'Item', 'Price'])

    driver.quit()
    return df
#%% applying scraping function to URLs

start_time = time.time()
urls = [
   'https://www.servis.pk/collections/men',
  'https://www.servis.pk/collections/cheetah'
]

df_combined = pd.DataFrame()
for url in urls:
    try:

        df_combined = \
            pd.concat([
                df_combined, scraping(url)
            ], axis = 0)
    except Exception as e:
        print(f"Error in scraping: {e}")
        df_combined = pd.DataFrame(columns=['url', 'Item', 'Price'])

end_time = time.time()
print(f"Scraping completed in {end_time - start_time} seconds")


# df_combined = pd.concat(results, ignore_index=True)
print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file)


# %%




# # -*- coding: utf-8 -*-
# """
# Created on Wed Oct 30 14:43:04 2024
# @author: Samia
# """

# %% Load Libraries
# import pandas as pd
# import numpy as np
# from datetime import datetime
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service as ChromeService
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# import os
# import time

# # %% Initialize the ChromeDriver
# def load_driver():  # Auto download Chrome Driver
#     chrome_options = Options()
#     chrome_options.add_argument("--start-maximized")
#     service = ChromeService(executable_path=ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service, options=chrome_options)
#     return driver

# def load_driver1():  # Manual download path for Chrome Driver
#     path = r"..\chromedriver-win64\chromedriver.exe"
#     chrome_options = Options()
#     chrome_options.add_argument("--start-maximized")
#     chrome_options.add_experimental_option("prefs", {
#         "profile.managed_default_content_settings.images": 2,
#         "profile.managed_default_content_settings.stylesheets": 2,
#     })
#     service = ChromeService(executable_path=path)
#     driver = webdriver.Chrome(service=service, options=chrome_options)
#     return driver

# # %% General Function to Get Products from Page
# def get_products(driver):
#     df = pd.DataFrame(columns=["Item", "Price"])
#     try:
#         wait = WebDriverWait(driver, 10)
#         wait.until(EC.presence_of_element_located((By.CLASS_NAME, "card__information")))

#         product_wrappers = driver.find_elements(By.CLASS_NAME, "card__information")
#         for wrapper in product_wrappers:
#             item = wrapper.find_element(By.CLASS_NAME, "full-unstyled-link").text
#             price_elements = wrapper.find_elements(By.CLASS_NAME, "price-item")
#             regular_price, sale_price = None, None
#             for price_element in price_elements:
#                 if "price-item--regular" in price_element.get_attribute("class"):
#                     regular_price = price_element.text.strip()
#                 elif "price-item--sale" in price_element.get_attribute("class"):
#                     sale_price = price_element.text.strip()
#             price = regular_price or sale_price or "Price Not Available"
#             if item and price:
#                 df.loc[len(df)] = {"Item": item, "Price": price}
#     except Exception as e:
#         print(f"Error fetching products: {e}")
#     return df

# # %% Scraping Function for `bata.com.pk`
# def scraping_bata(url):
#     driver = load_driver1()
#     driver.get(url)
#     time.sleep(3)
#     driver.execute_script("window.scrollBy(0, 500);")
#     time.sleep(3)
#     while True:
#         old_scroll = driver.execute_script("return window.pageYOffset;")
#         driver.execute_script("window.scrollBy(0, 500);")
#         time.sleep(0.5)
#         new_scroll = driver.execute_script("return window.pageYOffset;")
#         if new_scroll == old_scroll:
#             break
#     df = get_products(driver)
#     df['url'] = url
#     driver.quit()
#     return df

# # %% Scraping Function for `servis.pk`
# def scraping_servis(url):
#     driver = load_driver1()
#     driver.get(url)
#     time.sleep(3)
#     driver.execute_script("window.scrollBy(0, 500);")
#     time.sleep(3)
#     while True:
#         old_scroll = driver.execute_script("return window.pageYOffset;")
#         driver.execute_script("window.scrollBy(0, 500);")
#         time.sleep(0.5)
#         new_scroll = driver.execute_script("return window.pageYOffset;")
#         if new_scroll == old_scroll:
#             break
#     df = get_products(driver)
#     df['url'] = url
#     driver.quit()
#     return df

# # %% Run Scraping for Bata URLs
# start_time = time.time()
# bata_urls = [
#     "https://www.bata.com.pk/collections/ladies-sandals",
#     "https://www.bata.com.pk/collections/men-sandals",
#     "https://www.bata.com.pk/collections/men-chappals",
#     "https://www.bata.com.pk/collections/ladies-casual",
#     "https://www.bata.com.pk/collections/men-formal",
#     "https://www.bata.com.pk/collections/kids-sneaker"
# ]
# df_bata_combined = pd.DataFrame()
# for url in bata_urls:
#     df_bata_combined = pd.concat([df_bata_combined, scraping_bata(url)], ignore_index=True)

# # Save Bata results
# now = datetime.now().strftime("%Y-%m-%d_%H-%M")
# df_bata_combined.to_csv(f'../data/bata_{now}.csv', index=False)

# # %% Run Scraping for Servis URLs
# servis_urls = [
#     "https://www.servis.pk/collections/formal",
#     "https://www.servis.pk/search?q=y+ch+di+0050&_pos=1&_psq=Y-CH-DI&_ss=e&_v=1.0"
# ]
# df_servis_combined = pd.DataFrame()
# for url in servis_urls:
#     df_servis_combined = pd.concat([df_servis_combined, scraping_servis(url)], ignore_index=True)

# # Save Servis results
# df_servis_combined.to_csv(f'data/servis_{now}.csv', index=False)

# # %% Print Completion Time
# end_time = time.time()
# print(f"Scraping completed in {end_time - start_time:.2f} seconds")
