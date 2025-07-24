# Load Libraries
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

#%%

def open_menu(driver):
    while(1):
        driver.get("https://www.metro-online.pk/category/cooking-essentials/commodities/oil-and-ghee")
        try:
            #Open Menu
            element = driver.find_element(By.CLASS_NAME, "NewDesktopNav_menu_outlined_icon_container__8Lrtz")
            element.click()
            break
        except:
            #Check if error then close popup
            element = driver.find_elements(By.CLASS_NAME , "dn-slide-deny-btn")
            if element:
                element[1].click()
            continue


#Get the List of pages from Sub Category Page 
def get_address(driver):
    element = driver.find_element(By.CSS_SELECTOR, ".sc-gKPRtg.jJzJeK")         
    lst_count  = element.find_elements(By.TAG_NAME, "a")
    lst = []
    for i in lst_count:
        lst.append(i.get_attribute("href"))
    return lst



#Ouptut Dataframe with products based  on the given list of web address
def get_products(driver,web_add):
    df  = pd.DataFrame(columns = ["Category","Product","Price","Status"])

    for web in web_add:
        try:
            print(web)
            driver.get(web)
            while(1):
                old_scroll = driver.execute_script("return window.pageYOffset;")
                driver.execute_script("window.scrollBy(0, 400);")
                new_scroll = driver.execute_script("return window.pageYOffset;")
                p_count = len(driver.find_elements(By.CLASS_NAME , "CategoryGrid_product_card__FUMXW"))
                text = driver.find_elements(By.CLASS_NAME , "product_num_of_products__6uyYB")[0].text
                load_p = int(text) if text.isdigit() else 0
                text = driver.find_elements(By.CLASS_NAME , "product_num_of_products__6uyYB")[1].text
                tot_p = int(text) if text.isdigit() else 1
                
                if load_p == tot_p:
                    break
                time.sleep(1)
            
            product = driver.find_elements(By.CLASS_NAME , "CategoryGrid_product_card__FUMXW")
            for p in product:
                raw = p.text
                values = raw.split("\n")
                row = ([web] + values + [''] * (3 - len(values)))
                df.loc[len(df)] = row 
        except:
            print("Error in this page")
            continue
    print("Page done")
    return df

#%%

driver = load_driver1()
df_main  = pd.DataFrame(columns = ["Category","Product","Price","Status"])
open_menu(driver)
print("Menu Opened")
#Main Category
cat_element = driver.find_element(By.CLASS_NAME, "CategoryListingWeb_sidebar__F4PAk").find_elements(By.CLASS_NAME, "CategoryListingWeb_category_listing_container__wJJOm")
#Lopp for each main category:
cat_i = len(cat_element)
print("Categories loaded: ",cat_i)
retry = 0
for i in range (0,cat_i):
    try:
        open_menu(driver)
        cat_element = driver.find_element(By.CLASS_NAME, "CategoryListingWeb_sidebar__F4PAk").find_elements(By.CLASS_NAME, "CategoryListingWeb_category_listing_container__wJJOm")
        #Hover To Display Subcategory
        driver.execute_script("""
            var event = new Event('mouseover', { bubbles: true });
            arguments[0].dispatchEvent(event);
        """, cat_element[i])
        
        #Sub Category
        subcat_element = driver.find_elements(By.CLASS_NAME, "CategoryListingWeb_category_expanded_level_three_item__jjFUX")
        subcat_i = len(subcat_element)
        print(f"The value of sub cat: {subcat_i}")
        for j in range(0,subcat_i):
            open_menu(driver)
            cat_element = driver.find_element(By.CLASS_NAME, "CategoryListingWeb_sidebar__F4PAk").find_elements(By.CLASS_NAME, "CategoryListingWeb_category_listing_container__wJJOm")
            #Hover To Display Subcategory
            driver.execute_script("""
                var event = new Event('mouseover', { bubbles: true });
                arguments[0].dispatchEvent(event);
            """, cat_element[i])
            subcat_element = driver.find_elements(By.CLASS_NAME, "CategoryListingWeb_category_expanded_level_three_item__jjFUX")
            print(subcat_element[j].text)  #Delete
            subcat_element[j].click()
            time.sleep(2)
            check_product = driver.find_elements(By.CSS_SELECTOR, ".sc-gKPRtg.jJzJeK")
            pag_lst = []
            if check_product:
                pag_lst = get_address(driver)
            else:
                pag_lst = [driver.current_url]
            print(len(pag_lst))
            df = get_products(driver,pag_lst)
            df_main = pd.concat([df_main, df], ignore_index=True)
            print(len(df_main))
    except:
        print("Exception Raised")
        if retry < 5:
            retry = retry + 1
            i = i-1
            continue
        else:
            print("Error in Scraping")
            break
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_main.to_csv(file)