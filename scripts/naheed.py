#%%
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
from selenium.common.exceptions import SessionNotCreatedException
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

#%%

def scraping(comb):
    
    for attempt in range(3):
        try:
            driver = load_driver1()
        except SessionNotCreatedException as e:
            time.sleep(2)
            print("Driver load Attempt")
            continue
        except :
            print("Unable to load driver")
            driver.close()
        finally:
            break
    #Reading Data
    df =  pd.DataFrame(columns = ["Subcat","Category","Product","Price"])
    for index, row in comb.iterrows():
        urll = row['URL'] 
        cat = row['Category']
        new_page = True
        urln = urll
        pageno = 2
        #Check page
                        
        while new_page:
            try:
                delay = random.uniform(0, 2)  # Random delay between 1 and 5 seconds
                time.sleep(delay)
                driver.get(urln)
                while True:    
                    #Check 404 error
                    error_check = driver.find_elements(By.CLASS_NAME, "page-wrapper")
                    if error_check:
                        break
                    else:
                        time.sleep(30)
                        driver.refresh()
                        continue   
                wait = WebDriverWait(driver, 3)
                x = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'breadcrumbs')))
                try:
                    check = driver.find_element(By.CLASS_NAME, "sub-category-list")
                    if check:
                        #print("sub Category Page")
                        break
                except:
                    click = driver.find_elements(By.ID, "button-1")
                    if click:
                        click[0].click()
                    subcat = driver.find_element(By.CLASS_NAME, "breadcrumbs").text
                    sub_elements = driver.find_elements(By.CLASS_NAME, 'product-item-info')
                    #for element in sub_elements:
                        #prods = element.find_elements(By.CLASS_NAME, "product-item-details")
                    for prod in sub_elements:
                        p_name = prod.find_element(By.CLASS_NAME, "product-item-name").text
                        price = prod.find_element(By.CLASS_NAME, "price-final_price").text
                        df.loc[len(df)] = {'Subcat' : subcat , 'Category' : cat, 'Product' : p_name, 'Price': price}
            except Exception as e:
                #print("Page Not Found")
                #print(e)
                print(urll)
                break
            nextpage = driver.find_elements(By.CLASS_NAME, "pages-item-next")
            if nextpage:
                #print('new page')
                urln = urll + "?p=" + str(pageno)
                pageno = pageno + 1
            else:
                #print(urll)
                print(index)
                break
    driver.close()
    return df

#%%
try:
    driver = load_driver1()
    driver.get("https://www.naheed.pk")
    #Wait for page to load
    wait = WebDriverWait(driver, 20)
    x = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'dd-trigger')))
except:
    print("Main Page Not Found")
    driver.close()
    sys.exit(1)
#Dataframe to store Category name and url
df_parent_cat = pd.DataFrame(columns = ["Category","URL"])
element = driver.find_element(By.CLASS_NAME, "dd-trigger")

driver.execute_script("arguments[0].click();", element)
#element.click()
elements = driver.find_element(By.CLASS_NAME, "dd-content")
url = elements.find_elements(By.TAG_NAME, 'a')
for element in url:
    df_parent_cat.loc[len(df_parent_cat)] = {'URL' : element.get_attribute('href'), 'Category' : element.get_attribute("title")}
driver.close()


#%% 

start_time = time.time()
df_final = pd.DataFrame(columns = ["Subcat","Category","Product","Price"])
#df_parent_cat = df_parent_cat.loc[:200,:]
df_parent_cat.loc[len(df_parent_cat)] = ["https://www.naheed.pk/pharmacy/a-z", "Pharma"]
df_parent_cat = df_parent_cat.sample(frac=1, random_state=42).reset_index(drop=True)
chunk_size = 5
rows_perchunk = len(df_parent_cat)//chunk_size
# Split DataFrame into an array of DataFrames
df_list = [df_parent_cat.iloc[i:i + rows_perchunk] for i in range(0, rows_perchunk*4, rows_perchunk)]
df_list.append(df_parent_cat.iloc[(rows_perchunk*4):])
#Running code with 5 threads
with ThreadPoolExecutor(max_workers=5) as p:
    results = list(p.map(scraping, df_list))
df_combined = pd.concat(results, ignore_index=True)
time.sleep(20)
print(f"{(time.time() - start_time):.2f} seconds")    
now = datetime.now()
now = now.strftime("%Y-%m-%d_%H-%M")
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file)

#%%
