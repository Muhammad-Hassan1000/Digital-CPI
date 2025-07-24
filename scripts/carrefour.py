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
import os
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
#     chrome_options.add_argument("--disable-blink-features=AutomationControlled")
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

# %%
def hover_cat(driver, i = 0):
    try:
        wait = WebDriverWait(driver, 10)
        x = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@data-testid="all_categories"]'))) # Wait for allcategories bar to apear 
        all_cat = driver.find_element(By.XPATH, '//*[@data-testid="all_categories"]')
        actions = ActionChains(driver)
        actions.move_to_element(all_cat).perform()
        cat = driver.find_elements(By.CSS_SELECTOR, 'ul.css-9fgw80')[0].find_elements(By.CSS_SELECTOR, 'li.css-1bbzpo4')
        actions = ActionChains(driver)
        # Perform hover action
        actions.move_to_element(cat[i]).perform()
        return len(cat)
    except:
        try:
            driver.get("https://www.carrefour.pk/mafpak/en/")
        except:
            return 0




# def hover_cat(driver, i=0):
#     try:
#         wait = WebDriverWait(driver, 10)
#         print("Waiting for All Categories button...")
#         all_cat_button = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@data-testid="all_categories"]')))
        
#         print("Hovering over All Categories...")
#         actions = ActionChains(driver)
#         actions.move_to_element(all_cat_button).perform()

#         print("Waiting for categories to appear...")
#         wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'a[data-testid="category_level_1"]')))

#         categories = driver.find_elements(By.CSS_SELECTOR, 'a[data-testid="category_level_1"]')
#         print(f"Found {len(categories)} categories.")

#         if i < len(categories):
#             print(f"Hovering over category index {i}")
#             actions.move_to_element(categories[i]).perform()

#         return len(categories)

#     except Exception as e:
#         print("Error in hover_cat:", str(e))
#         try:
#             driver.save_screenshot("hover_error.png")
#             print("Saved screenshot for debugging.")
#             driver.get("https://www.carrefour.pk/mafpak/en/")
#         except Exception as e_reload:
#             print("Error reloading homepage:", str(e_reload))
#         return 0



# %%
def get_products(driver):
    df = pd.DataFrame(columns = ["Item","Price"])
    try:
        wait = WebDriverWait(driver, 5)
        x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-11qbfb'))) 
        prods = driver.find_elements(By.CSS_SELECTOR, 'div.css-11qbfb')
        prods = prods[-50:]
        for prod in prods:
            price  = prod.find_element(By.XPATH, './/*[@data-testid="product_price"]').text.replace('\n',' ')
            item1  = prod.find_element(By.XPATH, './/*[@data-testid="product_name"]').text.replace('\n',' ')
            item2 = prod.find_element(By.CSS_SELECTOR, 'div.css-1tmlydx').text.replace('\n',' ')
            df.loc[len(df)] = {"Item" : item1 + " " + item2 , "Price" : price}
        return df
    except:
        return df
def change_add(driver):
    driver.find_elements(By.CLASS_NAME, "css-ekjjru")[0].click()
    time.sleep(1)
    search_box = driver.find_elements(By.CLASS_NAME, "css-1asaqib")
    search_box[0].send_keys("lucky one mall r")
    time.sleep(5)
    search_box[0].send_keys("lucky one mall r")
    time.sleep(1)
    actions = ActionChains(driver)
    actions.move_to_element(search_box[0]).move_by_offset(0, 80).click().perform()
    add = driver.find_elements(By.CLASS_NAME, "css-q11lye")
    add[0].click()
    return

# %%
def scraping(list_rng):
    x,y = list_rng[0] , list_rng[1] 
    df = pd.DataFrame(columns = ["Category" , "Item" , "Price"])
    driver = load_driver1()
    driver.get('https://www.carrefour.pk/mafpak/en/')
    #change_add(driver)
    time.sleep(1)
    driver.find_elements(By.CLASS_NAME, 'css-fbcqx2')[0].find_elements(By.TAG_NAME , 'span')[0].click()
    time.sleep(1)
    driver.find_element(By.XPATH, './/*[@data-testid="Express Delivery"]').click()   #New addition
    time.sleep(1)
    for i in range(x,y):
        hover_cat(driver, i)
        try:
            subcat = driver.find_elements(By.XPATH, '//*[@data-testid="category_level_2"]')
            subcat_no = len(subcat)
        except:
            continue
        for j in range(0,subcat_no):
            hover_cat(driver, i)
            try:
                subcat = driver.find_elements(By.XPATH, '//*[@data-testid="category_level_2"]')
                subcat[j].click()
                #Wescrapping Start here
                cat_subcat_e = driver.find_elements(By.CSS_SELECTOR , 'ol.css-1jrk2hc')  #Adding "?"  to split the subcategories
                if cat_subcat_e:
                    cat_subcat = cat_subcat_e[0].text.replace('\n' , '?') + ":"
                else:
                    cat_subcat = ":"
                print(f'{cat_subcat}; {datetime.now().strftime("%H:%M")}')
                while True:
                    while True:
                        old_scroll = driver.execute_script("return window.pageYOffset;")
                        driver.execute_script("window.scrollBy(0, 500);")
                        time.sleep(0.25)
                        driver.execute_script("window.scrollBy(0, 500);")
                        time.sleep(0.25)
                        driver.execute_script("window.scrollBy(0, 500);")
                        time.sleep(0.25)
                        df_sub = get_products(driver)
                        if len(df_sub)>0:
                            df_sub['Category'] = cat_subcat
                            df = pd.concat([df,df_sub], ignore_index=True)
                        new_scroll = driver.execute_script("return window.pageYOffset;")
                        if new_scroll < old_scroll+500:
                            break
                    load_button = driver.find_elements(By.XPATH, '//*[@data-testid="trolly-button"]')
                    if load_button:
                        time.sleep(1)
                        load_button[0].click()
                        item_name =  df.iloc[len(df)-1,1]
                        try:
                            while (df.iloc[len(df)-1,1] == item_name):
                                prods  = driver.find_elements(By.CSS_SELECTOR, 'div.css-11qbfb')
                                item1  = prods[len(prods)-1].find_element(By.XPATH, './/*[@data-testid="product_name"]').text.replace('\n',' ')
                                item2  = prods[len(prods)-1].find_element(By.CSS_SELECTOR, 'div.css-1tmlydx').text.replace('\n',' ')
                                item_name =  item1 + " " + item2
                                time.sleep(0.5)
                        except:
                            time.sleep(2)
                    else:
                        break
                df = df.drop_duplicates(subset=['Item'])
            except:
                print("Subcategory Not Found")
                continue

    driver.quit()
    return df

# %%
start_time = time.time()

driver = load_driver1()
driver.get('https://www.carrefour.pk/mafpak/en/')
driver.find_elements(By.CLASS_NAME, 'css-fbcqx2')[0].find_elements(By.TAG_NAME , 'span')[0].click()
#driver.find_element(By.XPATH, '//div[.//span[text()="Express Delivery"]]').click()
#driver.find_element(By.XPATH, './/*[@data-testid="Express Delivery"]').click()

cat_no = hover_cat(driver, 0)
print(cat_no)
driver.quit()
list_range = [(0,cat_no//3 + 1),(cat_no//3 + 1,(cat_no//3 )* 2 +1),((cat_no//3 )* 2 +1 ,cat_no)]#Creating tuples for multithreading
#Running code with 5 threads
with ThreadPoolExecutor(max_workers = 3) as p:
    results = list(p.map(scraping, list_range))

df_combined = pd.concat(results, ignore_index=True)



# Calculate the elapsed time
print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file)

