import dotenv
import os

dotenv.load_dotenv(dotenv_path="../")
DATA_DIR = os.getenv("DATA_DIR")

# %% Import libraries 
import pandas as pd
import numpy as np
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
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

# %% URLs to scrape
urls = [
    "https://www.bariandson.com/product/request-a-tailor/",  # Bari and Sons URL for tailoring charges
]

# %% Function to scrape data
# Function to scrape data
def scrape(url):
    driver = load_driver1()
    time.sleep(1)
    driver.get(url)

    # DataFrame to hold the results
    df = pd.DataFrame(columns=['item', 'Price', 'url'])

    if 'bariandson' in url:
        time.sleep(5)
        try:
            # Find all containers for the tailoring options
            tailoring_containers = driver.find_elements(By.XPATH, "//div[@class='ywapo_input_container ywapo_input_container_checkbox']")

            # Loop through each container to extract label and data-price
            for container in tailoring_containers:
                # Extract the label text
                label_element = container.find_element(By.XPATH, ".//span[@class='ywapo_option_label ywapo_label_position_after']")
                label_text = label_element.text.strip()

                # Find the checkbox input and extract the data-price attribute
                checkbox = container.find_element(By.XPATH, ".//input[@class='ywapo_input ywapo_input_checkbox ywapo_price_fixed']")
                data_price = checkbox.get_attribute("data-price")

                # Add the extracted data to the DataFrame
                if data_price:
                    df.loc[len(df)] = {"item": label_text, "Price": data_price, "url": url}
                    
        except Exception as e:
            print(f"Unable to locate the price element on Bari and Sons website: {e}")

    time.sleep(1)
    driver.quit()  # Close the driver
    return df

# %% Scraping data from the URLs
start_time = time.time()
dfs = []
for url in urls:
    df = scrape(url)
    dfs.append(df)

# Combine the data from all URLs
df_combined = pd.concat(dfs, ignore_index=True)

# Ensure the data directory exists
# data_directory = 'data/'
# if not os.path.exists(data_directory):
#     os.makedirs(data_directory)

# Save the combined data to a CSV file with the current date and time in the filename
now = datetime.now().strftime("%Y-%m-%d_%H-%M")  # Get the current date and time
# file = os.path.join(data_directory, f'tailoring_charges{now}.csv')  # Filename with date and time
# Added/Modified by IT
file = create_csv_path(now)
df_combined.to_csv(file, index=False)

print(f"Data scraped and saved to {file}")
print(f"Time taken: {(time.time() - start_time):.2f} seconds")