# %%
#Install dependencies
#!pip install selenium
#!pip install webdriver_manager
#!pip install pandas
#!pip install openpyxl

# %%
import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")

# Load Libraries
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
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor
from selenium.common.exceptions import SessionNotCreatedException


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

# %%
def scraping(df_add):
#    urll, subcat, cat = comb
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
    #Scraping the price for each subcategory
    df =  pd.DataFrame(columns = ["Subcat","Category","Product","Price"])
    for index,row in df_add.iterrows():
        try:
            driver.get(row['URL'])
            wait = WebDriverWait(driver, 20)
            # Wait for a specific element to be present
            x = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'product-details')))
            cnt = 1
            while True:
            # Wait until the element is present (i.e., there is a "Load More" button)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                check = driver.find_elements(By.ID, 'shopify_section_template__22762958192928__product_grid_pagination')
                if check and (cnt<15):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                    cnt +=cnt
                else:
                    break
            elements = driver.find_elements(By.CLASS_NAME, 'product-details')
            for element in elements:
                p_name = element.find_element(By.TAG_NAME, 'a').text
                price = element.find_element(By.CLASS_NAME, 'product-price').text
                df.loc[len(df)] = {'Subcat' : row['Subcat'] , 'Category' : row['Category'], 'Product' : p_name, 'Price': price}
        except Exception as e:
            print("Page Not Found")
            print(row['Subcat'])
            continue

    return df


# %%
start_time = time.time()
try:
    driver = load_driver1()
    driver.get("https://alfatah.pk")
    time.sleep(15)
    #Wait for page to load
    wait = WebDriverWait(driver, 10)
    x = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'andaz_custom_menu')))
    #input()
    #Dataframe to store Category name and url
    df_parent_cat = pd.DataFrame(columns = ["Category","URL"])
    # Clik the Department to get different Categories; First element with class name "andaz_custom_menu"
    element = driver.find_element(By.CLASS_NAME, "andaz_custom_menu")
    driver.execute_script("document.getElementById('intellicon-chat-bot-iframe').style.display = 'none';")
    element.click()
    # Get the name and URLs of all the categories
    elements = driver.find_elements(By.CLASS_NAME, "andaz_parent_menu")
    for element in elements:
        url = element.find_element(By.TAG_NAME, 'a')
        df_parent_cat.loc[len(df_parent_cat)] = {'URL' : url.get_attribute('href'), 'Category' : url.text}
    #Deleting last empty column
    mask = (df_parent_cat == "")
    df_parent_cat = df_parent_cat[~mask.any(axis=1)]
    #Deleting Medicine because it douesnot have any sub cat
    df_parent_cat = df_parent_cat.drop(3)
    df_sub_cat =  pd.DataFrame(columns = ["Subcat","Category","URL"])
    #Using each Category URL to get the subcategory URLs
    for index,row in df_parent_cat.iterrows():
        try:
            driver.get(row['URL'])
            #Wait for page to load
            wait = WebDriverWait(driver, 20)
            x = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'box')))
            #Start getting sub cat addresses
            elements = driver.find_elements(By.CLASS_NAME, "box")
            for element in elements:
                url = element.find_element(By.TAG_NAME, 'a')
                df_sub_cat.loc[len(df_sub_cat)] = {'Subcat' : url.text, 'Category' : row['Category'], 'URL' : url.get_attribute('href')}
        except:
            continue
    df_sub_cat = df_sub_cat.drop_duplicates(subset=['URL'])
    driver.close()
    links = df_sub_cat['URL'].to_numpy()
    subcat = df_sub_cat['Subcat'].to_numpy()
    cat = df_sub_cat['Category'].to_numpy()
    #Creating tuples for multithreading
    combined  = list(zip(links,subcat,cat))
    # Number of  split
    chunk_size = 10
    rows_perchunk = len(df_sub_cat)//chunk_size
    # Split DataFrame into an array of DataFrames
    df_list = [df_sub_cat.iloc[i:i + rows_perchunk] for i in range(0, rows_perchunk*9, rows_perchunk)]
    df_list.append(df_sub_cat.iloc[(rows_perchunk*9):])
    #Running code with 10 threads
    with ThreadPoolExecutor(max_workers=10) as p:
        results = list(p.map(scraping, df_list))
    df_combined = pd.concat(results, ignore_index=True)
    print(f"{(time.time() - start_time):.2f} seconds")    
    now = datetime.now()
    now = now.strftime("%Y-%m-%d_%H-%M")
    # Added/Modified by IT
    file = create_csv_path(now)
    df_combined.to_csv(file)
except:
    print("Main Page Not Found")
    driver.close()

# %%



