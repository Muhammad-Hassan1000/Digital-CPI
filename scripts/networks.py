# %% Import libraries
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
import re
import time
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
    
# %% Initialize the ChromeDriver
# def load_driver():  # Auto download Chrome Driver
#     chrome_options = Options()
#     chrome_options.add_argument("--start-maximized")
#     # chrome_options.add_argument("--headless")  # Run in headless mode if required
#     service = ChromeService(executable_path=ChromeDriverManager().install())
#     driver = webdriver.Chrome(service=service, options=chrome_options)
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

# %% URLs to scrape
urls = [
    "https://www.ufone.com/uwon/",  # Ufone URL
    "https://jazz.com.pk/prepaid/price-plans",  # Jazz URL
    "https://www.zong.com.pk/onlineshop/zong-tariffs#:~:text=Zong%20Tariffs&text=Get%20call%20rate%20%40Rs2.,tax%2C%20Internet%20charges%20are%20Rs."  # Zong URL
]

# %% Function to extract numeric values
def extract_number(text):
    match = re.findall(r'\d+\.\d+', text)
    return match[0] if match else None

# %% Function to scrape data
def scrape(url):
    driver = load_driver1()
    time.sleep(1)
    driver.get(url)

    # DataFrame to hold the results
    df = pd.DataFrame(columns=['item', 'price', 'url'])

    if 'ufone' in url:
        time.sleep(5)
        # Locate the updated price element for Ufone using CSS selector
        try:
            ufone_price_element = driver.find_element(By.CSS_SELECTOR, "td.tblelft[width='45%']")
            ufone_price_text = ufone_price_element.text.strip()  # Extract the text from the element
            price_value = extract_number(ufone_price_text)  # Use your custom function to extract the number
            if price_value:
                df.loc[len(df)] = {"item": "Ufone Call Rate", "price": price_value, "url": url}
        except:
            print("Unable to locate the price element on Ufone website")

    elif 'jazz.com.pk' in url:
        time.sleep(5)
        # Locate the price elements for Jazz using CSS locator
        try:
            price_element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.text-sm")))
            price_text = price_element.text.strip()
            price_value = extract_number(price_text)
            if price_value:
                df.loc[len(df)] = {"item": "Jazz Call Rate", "price": price_value, "url": url}
        except:
            print("Unable to locate the price element on Jazz website")

    elif 'zong.com.pk' in url:
        time.sleep(5)
        # Locate the price elements for Zong using CSS selector
        try:
            zong_element = driver.find_element(By.CSS_SELECTOR, "div.plan_detail_row_single")
            zong_text = zong_element.text.strip()
            price_value = extract_number(zong_text)  # Extract the numeric price (e.g., Rs2.68)
            if price_value:
                df.loc[len(df)] = {"item": "Zong Call Rate", "price": price_value, "url": url}
        except:
            print("Unable to locate the price element on Zong website")

    time.sleep(1)
    driver.quit()  # Close the driver
    return df


# %% Scraping data from the URLs
start_time = time.time()
dfs = []
for url in urls:
    df = scrape(url)
    dfs.append(df)

# Combine the data from all URLs
df_combined = pd.concat(dfs, ignore_index=True)

# Ensure the data directory exists
# data_directory = 'data/'
# if not os.path.exists(data_directory):
#     os.makedirs(data_directory)

# Save the combined data to a CSV file with the current date and time in the filename
now = datetime.now().strftime("%Y-%m-%d_%H-%M")  # Get the current date and time
# file = os.path.join(data_directory, f'call_charges{now}.csv')  # Filename with date and time
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file, index=False)

print(f"Data scraped and saved to {file}")
print(f"Time taken: {(time.time() - start_time):.2f} seconds")