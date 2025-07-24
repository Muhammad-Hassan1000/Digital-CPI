from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime


# Helper function to initialize the Chrome driver
def load_driver() -> webdriver.Chrome:
    """
    Initializes and returns a headless Chrome WebDriver with optimized settings.
    """
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


# @task(retries=3, retry_delay_seconds=60, log_prints=True)
def scrape_gold_silver_rates() -> pd.DataFrame:
    """
    Scrapes gold and silver rates from gold.pk and returns a combined DataFrame.
    """
    driver = load_driver()
    df = pd.DataFrame(columns=["Item", "Area", "Rate", "Desc"])
    try:
        # Scrape Gold
        driver.get("https://gold.pk/gold-rates-pakistan.php")
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'p.goldratehome')))
        main_rate = driver.find_element(By.CSS_SELECTOR, "p.goldratehome").text
        df.loc[len(df)] = {
            "Item": "Gold",
            "Area": "Pakistan",
            "Rate": main_rate,
            "Desc": "Pakistan 24 Karat Gold Rate per Tola"
        }
        city_elements = driver.find_elements(By.CSS_SELECTOR , "div.progress-table")[1].find_elements(By.CSS_SELECTOR , "div.table-row")
        for element in city_elements:
            area = element.find_element(By.XPATH, '(div)[3]').text.strip()
            rate = element.find_element(By.XPATH, '(div)[4]').text.strip()
            df.loc[len(df)] = {
                "Item": "Gold",
                "Area": area,
                "Rate": rate,
                "Desc": "24 Karat Gold Rate per Tola"
            }
        # Scrape Silver
        driver.get("https://gold.pk/pakistan-silver-rates-xagp.php")
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'p.goldratehome')))
        main_rate = driver.find_element(By.CSS_SELECTOR, "p.goldratehome").text
        df.loc[len(df)] = {
            "Item": "Silver",
            "Area": "Pakistan",
            "Rate": main_rate,
            "Desc": "Pakistan 24 Karat Silver Rate per Tola"
        }
        return df
    finally:
        driver.quit()


# @flow(flow_run_name="Gold-Price-Ingestion", retries=3, retry_delay_seconds=300, log_prints=True)
def daily_gold_silver_pipeline():
    """
    Prefect flow that runs the gold and silver scraping task.
    """
    df = scrape_gold_silver_rates()
    # TODO: insert df into ClickHouse using clickhouse-connect or clickhouse-driver
    # Save local CSV backup
    now = datetime.now().strftime("%Y-%m-%d_%H-%M")
    df.to_csv(f"data/gold_rate_pipeline_{now}.csv", index=False)
    return df

if __name__ == "__main__":
    daily_gold_silver_pipeline()
