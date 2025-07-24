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
#     chrome_options.add_argument("--disable-gpu")
#     chrome_options.add_argument("--no-sandbox")
#     chrome_options.add_argument("--disable-dev-shm-usage")
#     chrome_options.add_argument('--remote-debugging-pipe')

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
city_wise_links = {
    'khi': "https://www.foodpanda.pk/darkstore/r1vf/pandamart-bahadurabad/categories",
    'lhr': "https://www.foodpanda.pk/darkstore/h8wc/pandamart-model-town/categories",
    'Isb' : "https://www.foodpanda.pk/darkstore/clnz/pandamart-satellite-town/categories",
    
    # 'sialkot':url    
}
start_time = time.time()
df_city_wise = pd.DataFrame()
driver = load_driver1()
for city, link in city_wise_links.items():
    
    driver.get(link)
    # driver.get("https://www.foodpanda.pk/darkstore/r1vf/pandamart-bahadurabad/categories")
    #Check for popup
    time.sleep(2)
    wait = WebDriverWait(driver, 30)
    x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.navbar-slot')))
    pop_up = driver.find_elements(By.XPATH, '//*[@data-testid="no-address-modal-close-button"]')
    if pop_up:
        pop_up[0].click()
                
    df_cat_add = pd.DataFrame(columns = ['Category', 'URL'])
    wait = WebDriverWait(driver, 30)
    x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.category-cards-categories-all')))
    # Get the Category Element
    cat = driver.find_element(By.CSS_SELECTOR, 'div.category-cards-categories-all')
    list_cat = cat.find_elements(By.TAG_NAME , "a")
    for add in list_cat:
        df_cat_add.loc[len(df_cat_add)] = {'Category': add.text , 'URL' : add.get_attribute('href')}

    df_meta = pd.DataFrame(columns = ["SubCat", "Items" , "Cat_Items"])
    df = pd.DataFrame(columns = ["Category","SubCat","Item","Price"])
    for index,row in df_cat_add.iterrows():
        try:
            driver.get(row['URL'])
            time.sleep(2)
            capt = driver.find_elements(By.CSS_SELECTOR, 'div.px-captcha-container')
            if capt:
                input("Comple the Human Test")
            wait = WebDriverWait(driver, 30)
            x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.navbar-slot')))
            pop_up = driver.find_elements(By.XPATH, '//*[@data-testid="no-address-modal-close-button"]')
            if pop_up:
                pop_up[0].click()
                    
            wait = WebDriverWait(driver, 30)
            x = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@itemprop="itemListElement"]')))
            prods = driver.find_elements(By.XPATH, '//*[@itemprop="itemListElement"]')
            for prod in prods:
                item = prod.find_element(By.XPATH, './/*[@data-testid="groceries-product-card-name"]').text
                price = prod.find_element(By.XPATH, './/*[@data-testid="groceries-product-card-price"]').text
                df.loc[len(df)] = {"Category" : row['Category'] , 
                            "Item" : item , 
                            "Price" : price }
            try :
                sutcat_name = driver.find_elements(By.CSS_SELECTOR, 'div.darkstore-category-page-navigation')
                subcat_name = []
                if subcat_name:
                    for element in sutcat_name .find_elements(By.TAG_NAME, 'li'):
                        subcat_name.append(element.text)
                    no_cat = driver.find_element(By.CSS_SELECTOR, 'div.darkstore-category-page-content').find_elements(By.CSS_SELECTOR , 'ul.product-grid')
                    i = 0
                    for no in no_cat:
                        no_prod = no.find_elements(By.XPATH, './/*[@itemprop="itemListElement"]')
                        df_meta.loc[len(df_meta)] = {"SubCat" : subcat_name[i],
                                                "Items" : len(no_prod) ,
                                                "Cat_Items" : len(prods) } 
                        i = i + 1
                else:
                    df_meta.loc[len(df_meta)] = {"SubCat" : row['Category'],
                                        "Items" : len(prods) ,
                                        "Cat_Items" : len(prods) } 
        
            except:
                df_meta.loc[len(df_meta)] = {"SubCat" : row['Category'],
                                    "Items" : len(prods) ,
                                    "Cat_Items" : len(prods) } 
        except:
            continue
            print("Page Error")    
        print("Page Done")
    df['city'] = city
    df_city_wise = pd.concat([
        df_city_wise, 
        df
    ], axis = 0)
    time.sleep(5)
    # driver.close()

# Calculate the elapsed time
print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_city_wise.to_csv(file)

# %%
