import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")

#%% Load Libraries
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

#%% Medicines
med_keywords = ['septran', 'flagyl', 'ventolin', 'daonil', 'entox', 'amoxil', 
                'lederplex', 'phenergan', 'calpol', 'hydryllin', 'inderal', 
                'betnovate', 'glaxose']

driver = load_driver1()
df = pd.DataFrame(columns=['item', 'price'])

for med_kw in med_keywords:
    url = f"https://www.dvago.pk/search?search={med_kw}"
    driver.get(url)
    time.sleep(10)
    
    try:
        data = [x.text.split('\n') for x in driver.find_elements(By.CLASS_NAME, 'ProductCard_productContent__HFMgl')]
    except Exception as e:
        print(f'Cannot scrape due to {e}')
        data = []

    if data:
        items = [x[0] for x in data]
        price = [x[1] for x in data]
        data_dict = {"item": items, "price": price}
        df_individual = pd.DataFrame(data_dict)
        df = pd.concat([df, df_individual], axis=0)

df['store'] = 'dvago'


#%% Save medicines data
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
file = 'data/meds' + now + '.csv'
df.to_csv(file, index=False)

#%% Tests
urls = ["https://healthwire.pk/lab-tests/ecg-test",
        "https://healthwire.pk/pharmacy/medicine/evocheck-glucometer",
        "https://healthwire.pk/lab-tests/blood-glucose-fasting-test",
        "https://healthwire.pk/lab-tests/chest-x-ray-test",
        "https://healthwire.pk/lab-tests/ultrasound-kidney-ureter-bladder",
        "https://healthwire.pk/lab-tests/urine-complete-examination",
        "https://healthwire.pk/lab-tests/ultra-sound-usg-abdominal-usg-paeds",
        "https://healthwire.pk/lab-tests/ceruloplasmin-blood-test"]

tests = pd.DataFrame(columns=['Item', 'Price'])
driver = load_driver1()

for url in urls:
    driver.get(url)
    time.sleep(10)
    if 'evocheck-glucometer' not in url:
        info = driver.find_element(By.CLASS_NAME, 'price_detail')
        item = info.find_element(By.CSS_SELECTOR, "h4").text
        price = info.find_element(By.CSS_SELECTOR, "strong").text
    else:
        info = driver.find_element(By.CLASS_NAME, 'medi-detail-right')
        item = info.find_element(By.CSS_SELECTOR, "h1").text
        price = info.find_element(By.CLASS_NAME, "variant_price").text

    tests.loc[len(tests)] = {'Item': item, 'Price': price}
    tests['categ'] = 'medical tests'
    tests['store'] = 'healthwire.pk'
    time.sleep(1)

driver.close()

now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
file = 'data/med_tests' + now + '.csv'
tests.to_csv(file, index=False)



#%% Doctors

# Assuming load_driver1() is defined elsewhere
driver = load_driver1()
urls = ['https://www.marham.pk/doctors/karachi/general-physician']

# Initialize an empty DataFrame for doctors
df_individual_docs = pd.DataFrame(columns=['doc_name', 'price', 'venue'])

for url in urls:
    driver.get(url)
    time.sleep(10)  # You may want to replace this with a wait for specific elements

    # Scrape doctor cards
    cards = driver.find_elements(By.CSS_SELECTOR, ".mb-2.mr-10.product-card.cursor-pointer.selectAppointmentOrOc")

    # Extract doctor data
    doctor_data = {
        'doc_name': [x.get_attribute("data-docname") for x in cards],
        'price': [x.get_attribute("data-amount") for x in cards],
        'venue': [x.get_attribute("data-hospitalname") for x in cards]
    }

    # Create DataFrame from doctor data
    df_individual_docs = pd.DataFrame(doctor_data)

    # Additional price and labels information
    try:
        # Wait for price elements to load
        prices = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.generic-green.fs16.mb5'))
        )
        display_prices = [x.text.split('\n')[0] for x in prices]

        # Ensure the length matches
        if len(display_prices) == len(df_individual_docs):
            df_individual_docs['display_price'] = display_prices
        else:
            print(f"Warning: Mismatch in length of display_prices ({len(display_prices)}) and df_individual_docs ({len(df_individual_docs)})")

        # Scraping labels
        labels = driver.find_elements(By.CSS_SELECTOR, ".nomargin.w-65")
        print(f"Number of labels found: {len(labels)}")  # Debugging info

        # Conditional assignment for labels
        if labels:
            df_individual_docs['labels'] = [x.text for x in labels]
        else:
            print("No labels found, skipping assignment.")

    except Exception as e:
        print(f"An error occurred: {e}")

# Save doctors data to CSV
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
file = 'data/doctors_' + now + '.csv'
df_individual_docs.to_csv(file, index=False)

# Close the driver
driver.close()



#%% Dentist

# Assuming load_driver1() is defined elsewhere
driver = load_driver1()

# Define the URLs for procedures
urls = ['https://oladoc.com/pakistan/karachi/treatment/tooth-extraction']

# Initialize an empty DataFrame to store extracted details
df_procedure_details = pd.DataFrame(columns=['doctor_name', 'location', 'price'])

# Loop through each URL
for url in urls:
    driver.get(url)
    
    # Explicit wait to ensure the doctor cards are loaded
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".treatment-card .doc-name"))
        )
        
        # Find doctor name, location, and price elements
        doctor_name_elements = driver.find_elements(By.CSS_SELECTOR, ".procedure-doc-name-wrapper .doc-name a")
        location_elements = driver.find_elements(By.CSS_SELECTOR, ".procedure-location")
        price_elements = driver.find_elements(By.CSS_SELECTOR, ".desktop-price .treatmenprice")

        # Loop through each doctor card to extract data
        for doctor_name, location, price_info in zip(doctor_name_elements, location_elements, price_elements):
            # Extracting main details from each doctor card
            doctor_name_text = doctor_name.text
            location_text = location.text
            
            # Extracting the price
            try:
                # If the "Starting from" text exists, remove it
                price_text = price_info.find_element(By.CLASS_NAME, "starting-from").find_element(By.XPATH, "following-sibling::text()").strip()
            except Exception as e:
                # Fallback to extract the entire price text
                price_text = price_info.text.split("\n")[-1]  # Take the last line as the main price

            # Append the data to the DataFrame
            df_procedure_details = pd.concat([
                df_procedure_details, 
                pd.DataFrame([{
                    'doctor_name': doctor_name_text,
                    'location': location_text,
                    'price': price_text
                }])
            ], ignore_index=True)

    except Exception as e:
        print(f"An error occurred while processing {url}: {e}")

# Save the procedure details to CSV
now = datetime.now().strftime("%Y-%m-%d_%H-%M")
file = f'data/Dentist_details_{now}.csv'
df_procedure_details.to_csv(file, index=False)

# Close the driver after finishing
driver.quit()
#%%
