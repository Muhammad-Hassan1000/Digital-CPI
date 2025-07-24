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
import time
import sys
import itertools
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
    # return driver
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
def scraping(combined):
    comb , check_creation = combined
    driver = load_driver1()
    last_data = []
    col = ['City','Location','Title','Area','Accomodation','Bed','Bath','Price','Trusted','Verified','Creation' , 'Updation']
    df = pd.DataFrame(columns = col)
    last_value = []
    webadd = 'https://www.zameen.com/' + comb[0] + comb[1] + '{}.html?sort=date_desc'
    #print(webadd)
    pageno = 1
    while (True):
        try:
            web = webadd.format(pageno)
            print(web)
            driver.get(web)
            lastpage = driver.find_elements(By.CSS_SELECTOR, '[aria-label="No hits box"]')
            if len(lastpage) !=0:
                break
            wait = WebDriverWait(driver, 10)
            x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[aria-label="Listing"]')))
        except:
            pageno = pageno + 1
            print("Error in page")
            continue
        elements = driver.find_elements(By.CSS_SELECTOR, '[aria-label="Listing"]')
        for element in elements:
            value = []
            try:
                trust = 1
                verified = 1
                if (len(element.find_elements(By.CSS_SELECTOR, '[aria-label="Trusted badge"]')) == 0):
                    trust = 0
                if (len(element.find_elements(By.CSS_SELECTOR, '[aria-label="Verified badge"]')) == 0):
                    verified = 0
    
                beds = element.find_elements(By.CSS_SELECTOR, '[aria-label="Beds"]')
                beds_value = beds[0].text if beds else np.nan
                baths = element.find_elements(By.CSS_SELECTOR, '[aria-label="Baths"]')
                baths_value = baths[0].text if baths else np.nan
                
                value.append(comb[1])
                value.append(element.find_element(By.CSS_SELECTOR, '[aria-label="Location"]').text)
                value.append(element.find_element(By.CSS_SELECTOR, '[aria-label="Title"]').text)
                value.append(element.find_element(By.CSS_SELECTOR, '[aria-label="Area"]').text)
                value.append(comb[0])
                value.append(beds_value)
                value.append(baths_value)
                value.append(element.find_element(By.CSS_SELECTOR, '[aria-label="Price"]').text)
                value.append(trust)
                value.append(verified)
                value.append(element.find_element(By.CSS_SELECTOR, '[aria-label="Listing creation date"]').text)
                value.append(element.find_element(By.CSS_SELECTOR, '[aria-label="Listing updated date"]').text)
                df.loc[len(df)] = value
                if value[10] in check_creation:
                    return df
            except:
                print("Error in Getting Values")
                continue
        pageno = pageno + 1
    driver.quit()
    return df                    

            

# %%
start_time = time.time()

location = ["Karachi-2-", "Lahore-1-","Islamabad-3-"]
acc_type = [ 'Rentals_Penthouse/', 'Rentals_Houses_Property/' , 'Rentals_Flats_Apartments/' , 'Rentals_Upper_Portions/' , 'Rentals_Lower_Portions/' , 'Rentals_Farm_Houses/']
combinations = list(itertools.product(acc_type,location))

check_creation = ['Added: 2 days ago','Added: 5 days ago','Added: 2 weeks ago']
print("Limit Scraping")
print("For 1 Day press 1 \nFor 4 Day press 2 \nFor 1 week  press 3 \nFor complete data press 0") 
# input_val = input("Give number 0-3")
input_val = 0
try:
    check = int(input_val)
    check_creation = check_creation[check-1:] if 1 <= check <= 3 else [] 
except:
    check_creation = []
combined = [(comb, check_creation) for comb in combinations]

with ThreadPoolExecutor(max_workers=3) as p:
    results = list(p.map(scraping, combined))

df_combined = pd.concat(results, ignore_index=True)
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file)
print(f"{(time.time() - start_time):.2f} seconds")    


# %%
# 6208.23/3600



