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
def load_address(driver,i=0): # i = 0 for Express and i =1 for standard
    try1 = 1
    driver.get("https://shop.imtiaz.com.pk/")
    while True:
        try:
            wait = WebDriverWait(driver, 20)
            x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button.blink-style-slpiel')))
            #Set Address
            #Set Standard Delivery
            element  = driver.find_elements(By.CSS_SELECTOR, 'button.blink-style-slpiel')
            element[i].click()
            #City Automatically Karachi 
            #Set Area As Dfence View Societ
            elements = driver.find_elements(By.CSS_SELECTOR, 'input.blink-style-pidix5')
            # loc = elements[i].location
            # x = loc['x'] 
            # y = loc['y'] 
            elements[i].send_keys("Defence View ")
            action = webdriver.common.action_chains.ActionChains(driver)
            action.move_to_element_with_offset(elements[i], 0, 30)
            action.click()
            action.perform()
            #Submit Address
            btn = driver.find_element(By.CSS_SELECTOR, "button.blink-style-3yb1hu")
            btn.click()
            return True
            # Address Done  
        except:
            if try1 < 4:
                try1 = try1 + 1
                driver.refresh()
                print("Retrying to load the main page")

                continue
            else:
                return False

# %%
def scraping(websites):
    driver = load_driver1()
    delay = random.uniform(0, 3)  # Random delay between 1 and 5 seconds
    time.sleep(delay)
    #Call function to load address
    if not load_address(driver,0):
        print("Failed to load the main page retry after some time")
        return df
    
    #Scraping the price for each subcategory
    #Load Each category URL
    
    
    df =  pd.DataFrame(columns = ["Category","URL","Product","Price"])
    for url in websites:
        try2 = 1
        while True:
            try:
                driver.get(url)
                wait = WebDriverWait(driver, 10)
                x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ol.MuiBreadcrumbs-ol.blink-style-nhb8h9"))) #Element for the Category name on top
                
            except:
                if try2 < 4:
                    try2 = try2 + 1
                    continue
                else:
                    print(url)
                    print("Error")
                    break 
                    
            i = 3
            while True: #Section Loop
                try:
                    cat = driver.find_element(By.CSS_SELECTOR, "ol.MuiBreadcrumbs-ol.blink-style-nhb8h9").text.replace('\n', ' ') #Element for the Category name on top
                    while True: #Page Loop
                        try:
                            #prod_class = 'blink-style-1zevr1
                            time.sleep(2)
                
                            #wait = WebDriverWait(driver, 25)
                            #x = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "blink-style-1a5qh6e"))) # Wait for product container to load
                            prod_class = 'blink-style-5bkk4b' #Div class have the product details blink-style-1a5qh6e
                            wait = WebDriverWait(driver, 20)
                            x = wait.until(EC.presence_of_element_located((By.CLASS_NAME, prod_class))) # Wait for product container to load
                            
                            prods = driver.find_elements(By.CLASS_NAME, prod_class)
                            for prod in prods:
                                prod_det = prod.find_element(By.CSS_SELECTOR, "div.hazle-product-item_product_item_text_container__Apuq1") #Element having product name and price
                                p_name = prod_det.find_element(By.TAG_NAME, 'p').text 
                                price = prod_det.find_element(By.TAG_NAME, 'span').text
                                df.loc[len(df)] = {'Category' : cat, "URL" : driver.current_url , 'Product' : p_name, 'Price': price}
                        except Exception as e:
                            print(driver.current_url)
                            print("No Data Found")
                        check = driver.find_elements(By.CSS_SELECTOR , "button.blink-style-1qdlk02")  # Next button class
                        # Check if next button for new page
                        if check:  
                            check1 = check[1]
                            is_disabled = check1.get_attribute('disabled') # if Next button disble or enable
                            if is_disabled:
                                break
                            else:
                                check1.click() # Goto next page
                                
                        else:
                            break
                    pages = driver.find_elements(By.CSS_SELECTOR, 'ul.blink-style-hqybzm') # Section address to check for difffere sub cats
                    if len(pages) >= 2:
                        seemore = driver.find_elements(By.CSS_SELECTOR, "a.blink-style-1hn530")
                        if seemore:
                            if  seemore[0].text == "See more":
                                seemore[0].click()
                        sec_select = pages[0].find_elements(By.TAG_NAME, 'li')
                        if len(sec_select)   >= i:
                            for j in range(1,i):
                                sec_select.pop(0)
                            i = i + 1
                            current_url = driver.current_url
                            action = webdriver.common.action_chains.ActionChains(driver)
                            action.move_to_element(sec_select[0].find_element(By.TAG_NAME, 'input'))
                            action.click()
                            action.perform() 
                            
                            u_url = driver.current_url
                            while current_url == u_url:
                                time.sleep(1)
                                u_url = driver.current_url
                            time.sleep(2)
                        else:
                            break
                    else:
                        break
                except :
                    break
            print(url)
            print("Done")
            break
   
    return df

# %%
start_time = time.time()
df_combined =[]
driver = load_driver1()
driver.get("https://shop.imtiaz.com.pk/")
#Wait for page to load
if load_address(driver,0):
    
    df_web_add = pd.DataFrame(columns = ["URL"])
    wait = WebDriverWait(driver, 20)
    x = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.MuiBox-root.blink-style-18gt081"))) #Categories Web element
    time.sleep(3)
    element = driver.find_element(By.CSS_SELECTOR, "div.MuiBox-root.blink-style-18gt081") #Categories Web element
    element.click()
    actions = webdriver.common.action_chains.ActionChains(driver)
         # Move to the element and perform the hover action
    url = driver.find_element(By.CSS_SELECTOR, 'ul.MuiList-root.MuiList-padding.blink-style-1ynsjfa') #Categories Area to hover to generate the sub cat
    actions.move_to_element(url).perform()
    time.sleep(1) 
    urls = driver.find_element(By.CSS_SELECTOR, 'div.MuiBox-root.blink-style-18ypgbx').find_elements(By.TAG_NAME, 'a') #Div element having all the sub cate gories
    for url in urls:
        df_web_add.loc[len(df_web_add)] = {'URL' :url.get_attribute('href')}

    driver.close()
else:
    print("Main Page or Address Issue")

links = df_web_add['URL'].to_numpy()
websites_split = [df_web_add['URL'].iloc[i::3].tolist() for i in range(3)]

if len(links)>0:
    
    #Creating tuples for multithreading
    #Running code with 5 threads
    with ThreadPoolExecutor(max_workers = 3) as p:
        results = list(p.map(scraping, websites_split))
    
    df_combined = pd.concat(results, ignore_index=True)
    
    print(f"{(time.time() - start_time):.2f} seconds")    
    
    now = datetime.now()
    now = now.strftime("%Y-%m-%d_%H-%M")
    # Added/Modified by IT
    file = create_csv_path(now)
    df_combined.to_csv(file)
else:
    print("Rerun")
    driver.quit()

    


# %%



