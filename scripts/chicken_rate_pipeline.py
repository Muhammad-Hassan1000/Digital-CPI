from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime

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


def extract_date(driver: webdriver.Chrome) -> str:
    """
    Extracts and returns the scrape date in YYYY-MM-DD format.
    """
    heading = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, "//h2[contains(text(), 'Chicken rate today in Pakistan')]") )
    )
    date_text = heading.text
    # parse e.g. "Chicken rate today in Pakistan - April 23, 2025"
    parsed = datetime.strptime(date_text.split('-')[-1].strip(), "%B %d, %Y")
    return parsed.strftime("%Y-%m-%d")


def extract_data(driver: webdriver.Chrome, date: str) -> pd.DataFrame:
    """
    Parses page sections and returns a cleaned DataFrame of chicken rates.
    """
    sections = driver.find_elements(By.XPATH, "//div[@class='kt-inside-inner-col']")
    records = []
    for sec in sections:
        try:
            category = sec.find_element(By.XPATH, ".//h3/strong").text.strip()
            price_type = sec.find_element(By.XPATH, ".//p[contains(text(), 'price per Kg')]").text.strip()
            price = sec.find_element(By.XPATH, ".//p/strong").text.strip()
            records.append((category, price_type, price))
        except Exception as e:
            # Log and skip problematic section
            print(f"Warning: skipping section due to {e}")

    df = pd.DataFrame(records, columns=["category", "price_type", "price"]);
    df['product'] = df['category'] + '_' + df['price_type']
    df['date'] = date
    return df[['product', 'price', 'date']]


# @task(retries=3, retry_delay_seconds=60, log_prints=True)
def scrape_chicken_rates() -> pd.DataFrame:
    """
    Main scraping task: loads the driver, navigates, scrolls, clicks, extracts data, and returns a DataFrame.
    """
    driver = load_driver()
    try:
        driver.get("https://todayschicken.com/")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Scroll and click a central element to dismiss overlays or load content
        driver.execute_script("window.scrollBy(0, 700);")
        size = driver.get_window_size()
        abs_x = int(size['width'] * 0.5)
        abs_y = int(size['height'] * 0.5)
        ActionChains(driver).move_by_offset(abs_x, abs_y).click().perform()

        # Extract date and data
        date_str = extract_date(driver)
        df = extract_data(driver, date_str)

        # Save locally as CSV
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        df.to_csv(f"data/chicken_rate_pipeline_{timestamp}.csv", index=False)
        return df

    finally:
        driver.quit()


# @flow(flow_run_name="Chicken-Price-Ingestion", retries=3, retry_delay_seconds=300, log_prints=True)
def daily_chicken_pipeline():
    """
    Prefect flow that runs the chicken rate scraping task.
    """
    df = scrape_chicken_rates()
    # TODO: insert df into ClickHouse using clickhouse-connect or clickhouse-driver
    return df


if __name__ == "__main__":
    # daily_chicken_pipeline()
    scrape_chicken_rates()