#%%
import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")

import pandas as pd
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
from selenium.webdriver.common.action_chains import ActionChains
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

# %%
driver = load_driver1()
driver.get("https://todayschicken.com/")
time.sleep(3)
driver.execute_script("window.scrollBy(0, 660);")
time.sleep(0.25)

# Get window size
window_size = driver.get_window_size()
width, height = window_size['width'], window_size['height']

# Define normalized coordinates (0 to 1)
normalized_x = 0.5  # Example: Center of the screen
normalized_y = 0.5

# Convert to absolute pixel coordinates
absolute_x = int(normalized_x * width)
absolute_y = int(normalized_y * height)

# Perform the click
actions = ActionChains(driver)
actions.move_by_offset(absolute_x, absolute_y).click().perform()
# %%

from datetime import datetime

date_text = driver.find_element(By.XPATH, "//h2[contains(text(), 'Chicken rate today in Pakistan')]").text
date = datetime.strptime(date_text.split('-')[-1].strip(), "%B %d, %Y").strftime('%Y-%m-%d')

# Find all product sections
sections = driver.find_elements(By.XPATH, "//div[@class='kt-inside-inner-col']")

data_list = []

for section in sections:
    try:
        # Extract category (e.g., "Broiler alive chicken")
        category = section.find_element(By.XPATH, ".//h3/strong").text.strip()

        # Extract type (e.g., "Retail price per Kg", "Wholesale price per Kg")
        price_type = section.find_element(By.XPATH, ".//p[contains(text(), 'price per Kg')]").text.strip()

        # Extract price (e.g., "411 Rs.")
        price = section.find_element(By.XPATH, ".//p/strong").text.strip()

        data_list.append((category, price_type, price))
    except Exception as e:
        print(f"Error processing section: {e}")

# Print extracted data
for category, price_type, price in data_list:
    print(f"{category} - {price_type}: {price}")

df = pd.DataFrame(data_list, columns=['product1', 'product2', 'price'])
df['product'] = df['product1'] +'_'+ df['product2']
df['date'] = date
df = df.drop(['product1', 'product2'], axis = 1)
df

# Added/Modified by IT
now = datetime.now().strftime("%Y-%m-%d_%H-%M")
file = create_csv_path(now)
#%%
df.to_csv(file)
driver.close()
# %%
