
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
    chrome_options.binary_location = os.path.abspath("/usr/bin/chromium")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # service = Service()
    driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver', options=chrome_options)
    return driver



#%% 

driver = load_driver1()
driver.get("https://sabzimarket.online/")
elements = driver.find_elements(By.CLASS_NAME , "menu-link")




# %% Function to extract product details
def get_products(driver, addr):
    df = pd.DataFrame(columns=["Item", "Price", "Address"])
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.astra-shop-summary-wrap')))
        products = driver.find_elements(By.CSS_SELECTOR, 'div.astra-shop-summary-wrap')

        for product in products:
            try:
                item = product.find_element(By.CSS_SELECTOR, 'h2.woocommerce-loop-product__title').text
                price = product.find_element(By.CSS_SELECTOR, 'span.price > span.woocommerce-Price-amount.amount').text
                df = pd.concat([df, pd.DataFrame([{"Item": item, "Price": price, "Address": addr}])], ignore_index=True)
            except Exception as e:
                print(f"Error scraping product: {e}")
        return df
    except Exception as e:
        print(f"Error loading products: {e}")
        return df
    
    
    
    
#%%
# Main scraping logic
def scrape_sabzimarket():
    driver = load_driver1()
    try:
        driver.get("https://sabzimarket.online/")
        elements = driver.find_elements(By.CLASS_NAME, "menu-link")

        pages = [e.get_attribute('href') for e in elements]

        df = pd.DataFrame(columns=["Item", "Price", "Address"])

        for page in pages:
            try:
                driver.get(page)
                time.sleep(2)  # Allow the page to load
                df = pd.concat([df, get_products(driver, page)], ignore_index=True)
            except Exception as e:
                print(f"Error processing page {page}: {e}")

        # Save the scraped data
        now = time.strftime("%Y-%m-%d_%H-%M")
        # Added/Modified by IT
        file = create_csv_path(now)
        df.to_csv(file, index=False)
        print(f"Data scraped and saved to {file}")
    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        driver.quit()

# Run the scraper
if __name__ == "__main__":
    scrape_sabzimarket()
