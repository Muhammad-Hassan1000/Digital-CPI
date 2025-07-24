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
# import undetected_chromedriver as uc
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
import subprocess
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

# %% Initialize the ChromeDriver
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


user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

def load_driver2():
    options = Options()
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    return webdriver.Chrome(options=options)


def load_driver3():

    # Define the command as a list
    command = [
        "powershell",
        '-Command',
        '& "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\\chrome_debug_profile_new"'
    ]

    # Execute the command using subprocess
    subprocess.run(command)
    time.sleep(2)
    chromedriver_path = r"chromedriver-win64\chromedriver.exe"
    #chromedriver_path = r"C:\Users\Shoaib Alvi\OneDrive\New Work FY25\chromedriver-win64\chromedriver.exe"
    # chromedriver_path = r"C:\Users\Shoaib\OneDrive\New Work FY25\chromedriver-win64\chromedriver.exe"

    # Set up Chrome options to use the remote debugging port
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    # prefs = {
    #     "profile.managed_default_content_settings.images": 2,  # Block images
    #     "profile.managed_default_content_settings.stylesheets": 2  # Block stylesheets
    # }
    # chrome_options.add_experimental_option("prefs", prefs)

    # Connect to existing Chrome window
    driver = webdriver.Chrome(service=Service(executable_path=chromedriver_path,
                                            log_path='chromedriver.log'), 
                                            options=chrome_options)
    return driver


# %% Main scraping logic
def scrape_foodpanda_data():
    
    
    urls = [
        "https://www.foodpanda.pk/restaurant/s6gz/jeddah-foods-boat-basin",
        "https://www.foodpanda.pk/restaurant/t9yv/munawar-restaurant",
        "https://www.foodpanda.pk/restaurant/v4gn/nihari-inn-boat-basin",
        "https://www.foodpanda.pk/restaurant/w4bf/quetta-chai",
        "https://www.foodpanda.pk/restaurant/u6vf/sindh-sweets-khadda-market",
        "https://www.foodpanda.pk/chain/cw4xt/broadway-pizza",
        "https://www.foodpanda.pk/restaurant/g279/tipu-burgers-and-broast",
        "https://www.foodpanda.pk/restaurant/s2nl/dominos-pizza-zamzama",
        "https://www.foodpanda.pk/restaurant/pfjh/mardan-shahi-dera-koila-karahi-and-bar-b-q",
        "https://www.foodpanda.pk/chain/ca2ps/pehalwan-hotel",
        "https://www.foodpanda.pk/chain/cc6dz/new-malakand-hotel",
        "https://www.foodpanda.pk/restaurant/s2xy/shahid-shinwari",
        "https://www.foodpanda.pk/restaurant/t9yk/hassan-zai-koyla-karahi-sajji-and-bar-b-que",
        "https://www.foodpanda.pk/chain/ce9sf/dewan-karahi",
        
    ]

    dfs = []
    
    # Load driver once, instead of reloading for each URL
    driver = load_driver1()

    for url in urls:
        driver.get(url)
        
        # Wait for page to load, adjust timeout as needed
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.px-captcha-container'))
            )
        except Exception as e:
            print(f"Error waiting for CAPTCHA container: {e}")
        
        # Handle CAPTCHA if it appears
        capt = driver.find_elements(By.CSS_SELECTOR, 'div.px-captcha-container')
        if capt:
            print("Captcha required!")
            input("Complete the human test, then press Enter")

        # Wait for menu items to load
        wait = WebDriverWait(driver, 30)
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, '//button[@data-testid="menu-product-button-overlay-id"]')))
        except Exception as e:
            print(f"Error waiting for menu items: {e}")
            continue  # Skip to the next URL if an error occurs

        # Initialize DataFrame to store item and price data
        df = pd.DataFrame(columns=['item', 'price']) 
        
        # Fetch all the menu item buttons
        buttons = driver.find_elements(By.XPATH, '//button[@data-testid="menu-product-button-overlay-id"]')
        for button in buttons:
            aria_label = button.get_attribute('aria-label')
            if aria_label:
                item, price = aria_label.split('Rs.')
                price = price.replace(' - Add to cart', '').strip()
                df.loc[len(df)] = {"item": item, "price": price}
        
        # Add the URL (restaurant name) to the DataFrame
        df['url'] = os.path.basename(url).replace('-', ' ')
        
        # Append the DataFrame to the list
        dfs.append(df)

    # Close the driver
    driver.quit()
    
    # Return or process the DataFrames as needed
    return dfs

#%% Run the function
start_time = time.time()
scraped_data = scrape_foodpanda_data()

df = pd.concat(scraped_data, ignore_index=True)


print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df.to_csv(file)


# %%
