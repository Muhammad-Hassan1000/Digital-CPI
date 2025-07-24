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
    chrome_options.binary_location = os.path.abspath("/usr/bin/chromium")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # service = Service()
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver', options=chrome_options)
    return driver

# %% Function to scrape data

urls = [
    "https://fruitsabzi.pk/#" 
   
]

def scrape(url):
    # url = urls[0]
    try:
        driver = load_driver1()  # Initialize WebDriver
        if driver is None:
            print("Driver initialization failed.")
            return pd.DataFrame(columns=['item', 'Price', 'url'])

        driver.get(url)
        time.sleep(60)  # Allow the page to load

        # DataFrame to hold the results
        df = pd.DataFrame(columns=['item', 'Price', 'url'])

        try:
            # Locate all product elements
            product_elements = driver.find_elements(By.CSS_SELECTOR, "div.pizza-content-container.row")
            if not product_elements:
                print("No products found.")
                return df

            for product in product_elements:
                try:
                    # Extract product name from 'wrap-info'
                    name_element = product.find_element(By.CSS_SELECTOR, "div.wrap-info h2")
                    product_name = name_element.text.strip()


                    # Extract product price from 'normal-price'
                    price_element = product.find_element(By.CSS_SELECTOR, ".sale-price.has-discount")
                    product_price = price_element.text.strip()

                    # Append to DataFrame
                    df.loc[len(df)] = {
                        "item": product_name,
                        "Price": product_price,
                        "url": url
                    }
                except Exception as e:
                    print(f"Error extracting product details: {e}")
        except Exception as e:
            print(f"Error locating product elements: {e}")
    except Exception as e:
        print(f"Error initializing WebDriver or loading the page: {e}")
        return pd.DataFrame(columns=['item', 'Price', 'Description', 'url'])
    finally:
        if 'driver' in locals() and driver is not None:
            driver.quit()  # Ensure the driver quits only if it was initialized

    return df


# %%
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
    now = time.strftime("%Y-%m-%d_%H-%M-%S")
    # Added/Modified by IT
    file = create_csv_path(now)
    df_combined.to_csv(file, index=False)

    print(f"Data scraped and saved to {file}")
else:
    print("No data scraped.")

print(f"Time taken: {(time.time() - start_time):.2f} seconds")


#os.getcwd()

#print(os.path.abspath(Vegetables&Fruits))

# Print the full file path
#print(f"CSV file saved at: {os.path.abspath(file)}")
# %%



