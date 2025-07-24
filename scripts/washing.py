
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


# %% URLs to scrape
urls = [
    "https://snowhite.com.pk/v5/wordpress/all-pricing/"
]

# %% Function to extract numeric values
def extract_number(text):
    match = re.findall(r'\d+', text)
    return match[0] if match else None

# %% Function to scrape data
def scrape(url):
    driver = load_driver1()
    if driver is None:
        print("Driver initialization failed.")
        return pd.DataFrame(columns=['item', 'Price', 'url'])
    
    time.sleep(1)
    driver.get(url)

    # DataFrame to hold the results
    df = pd.DataFrame(columns=['item', 'Price', 'url'])

    # Scraping logic for the new URL
    if "snowhite.com.pk" in url:
        time.sleep(5)
        try:
            # Locate all elements matching the specific structure of interest
            price_elements = driver.find_elements(By.CSS_SELECTOR, "td.has-text-align-center[style='text-align: center;'][data-align='center']")
            
            # Loop through each element and extract data
            for price_element in price_elements:
                price_text = price_element.text.strip()
                price_value = extract_number(price_text)
                if price_value:
                    # Adjust the selector as necessary if the item name is not found
                    try:
                        item_name = price_element.find_element(By.XPATH, '..//preceding-sibling::td').text.strip()
                        df.loc[len(df)] = {"item": item_name, "price": price_value, "url": url}
                    except Exception as e:
                        print(f"Unable to locate item name for a price: {e}")
                    
        except Exception as e:
            print(f"Error locating price elements on Snow White page: {e}")

    time.sleep(1)
    driver.quit()  # Close the driver
    return df

# %% Scraping data from the URLs
start_time = time.time()
dfs = []
for url in urls:
    df = scrape(url)
    if not df.empty:
        dfs.append(df)

# Combine the data from all URLs if any data was collected
if dfs:
    df_combined = pd.concat(dfs, ignore_index=True)

    # Ensure the data directory exists
    # data_directory = 'data/'
    # os.makedirs(data_directory, exist_ok=True)

    # Save the combined data to a CSV file with the current date and time in the filename
    now = datetime.now().strftime("%Y-%m-%d_%H-%M")
    # Added/Modified by IT
    file = create_csv_path(now)
    df_combined.to_csv(file, index=False)

    print(f"Data scraped and saved to {file}")
else:
    print("No data scraped.")
    
print(f"Time taken: {(time.time() - start_time):.2f} seconds")


# %%
