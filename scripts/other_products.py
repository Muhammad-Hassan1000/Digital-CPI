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


def scroll_and_scrape(driver, url, sleeptime2=2):
    """
    Function to scroll the page and scrape products.

    Args:
    driver: Selenium WebDriver instance
    url: The URL to append to the scraped data
    sleeptime2: Time to wait between scroll actions (default is 2 seconds)

    Returns:
    DataFrame containing scraped product data with the URL included
    """

    # driver = load_driver1()
    driver.get(url)
    time.sleep(1)
    
    # Scroll the first 750px
    initial_scroll = 1000
    df = pd.DataFrame(columns=["Item", "Price"])

    old_scroll = driver.execute_script("return window.pageYOffset;")
    time.sleep(1)
    
    # scroll a set amount/px initially
    driver.execute_script(f"window.scrollBy(0, {initial_scroll});")
    time.sleep(1)
    
    new_scroll1 = driver.execute_script("return window.pageYOffset;")
    driver.execute_script("window.scrollBy(0, 500);")
    time.sleep(1)
    new_scroll2 = driver.execute_script("return window.pageYOffset;")
    print(f'scrolls - old: {new_scroll1}, new: {new_scroll2}')
    time.sleep(1)
    
    if new_scroll1 > (new_scroll2-500):
        # If the scroll doesn't update, refresh the page and try scrolling again
        print("Scroll did not update, refreshing the page...")
        driver.get(url)
        time.sleep(sleeptime2)
        
        while True:
            old_scroll = driver.execute_script("return window.pageYOffset;")
            print(old_scroll)
            print(f'sleeping for {sleeptime2} seconds')
            time.sleep(sleeptime2)  # Allow time for page to refresh
            
            # Scroll twice by 500px each time
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(sleeptime2)
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(sleeptime2)
            
            new_scroll_after_refresh = driver.execute_script("return window.pageYOffset;")
            print(new_scroll_after_refresh)
            time.sleep(sleeptime2)
            
            # Check if scrolling has stopped
            if new_scroll_after_refresh < old_scroll + 500:
                return scrape_items(driver, df, url)


            # # Scroll 750px again after refresh
            # driver.execute_script(f"window.scrollBy(0, {initial_scroll});")
            # time.sleep(10)
            # new_scroll_after_refresh = driver.execute_script("return window.pageYOffset;")
            # print(new_scroll_after_refresh)
            
            # # If after refreshing, the scroll doesn't update, scrape visible items and return
            # if new_scroll_after_refresh == old_scroll:
            #     print("Scroll still not updating after refresh, scraping visible items...")
            #     return scrape_items(driver, df, url)
        
    else:
        # Continue with normal scrolling logic
        while True:
            old_scroll = new_scroll2
            time.sleep(sleeptime2)
            
            # Scroll twice by 500px each time
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(sleeptime2)
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(sleeptime2)
            
            new_scroll2 = driver.execute_script("return window.pageYOffset;")
            
            # Check if scrolling has stopped
            if new_scroll2 < old_scroll + 500:
                return scrape_items(driver, df, url)

def scrape_items(driver, df, url):    
    try:
        prices = driver.find_elements(By.CLASS_NAME, 'CategoryGrid_product_price__Svf8T')
        items = driver.find_elements(By.CLASS_NAME, 'CategoryGrid_product_name__3nYsN')
        
        for item, price in zip(items, prices):
            df.loc[len(df)] = {"Item": item.text, "Price": price.text}
    except Exception as e:
        print(f'Cannot scrape due to {e}')
    
    df['url'] = url  # Add the URL to the dataframe
    return df

#%%

url = "https://www.metro-online.pk/store/fresh-food/seafood/fish"

driver = load_driver1()
# driver.get(url)

start_time = time.time()
fish_df = scroll_and_scrape(driver, url)
fish_df['store'] = 'metro-online'
fish_df['category'] = 'seafood-fish'

print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
fish_df.to_csv(file)