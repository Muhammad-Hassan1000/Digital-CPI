import sqlite3
from pathlib import Path
from datetime import datetime, date
import pytz
import os
import dotenv

dotenv.load_dotenv()
DATABASE_FILE_PATH = os.getenv("DATABASE_FILE_PATH")
METADATA_SOURCE_ID = os.getenv("METADATA_SOURCE_ID")
METADATA_SCRIPT_NAME = os.getenv("METADATA_SCRIPT_NAME")
SQLITE_TIMEOUT_SECONDS = os.getenv("SQLITE_TIMEOUT_SECONDS")
SQLITE_JOURNAL_MODE = os.getenv("SQLITE_JOURNAL_MODE")
SQLITE_SYNCHRONOUS_MODE = os.getenv("SQLITE_SYNCHRONOUS_MODE")


def initialize_db():
    """Create the SQLite file and the three tables if they do not exist.""" 
    db_path = Path(DATABASE_FILE_PATH)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS datasource (
        datasource_id            INTEGER PRIMARY KEY AUTOINCREMENT,
        datasource_name          TEXT NOT NULL,
        datasource_script_name   TEXT NOT NULL,
        avg_rows                 INTEGER NOT NULL,
        allowed_deviation        REAL NOT NULL,
        eff_start_dt             DATE NOT NULL,
        eff_end_dt               DATE
    );
                      
    CREATE TABLE IF NOT EXISTS status (
        status_id                INTEGER PRIMARY KEY AUTOINCREMENT,
        datasource_id            INTEGER NOT NULL,
        status                   TEXT NOT NULL,
        period                   DATE NOT NULL,
        scraped_rows             INTEGER DEFAULT 0,
        start_time               DATETIME,
        end_time                 DATETIME,
        duration_seconds         REAL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS logs (
        log_id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        datasource_id            INTEGER NOT NULL,
        status_id                INTEGER NOT NULL,
        timestamp                DATETIME NOT NULL,
        log_type                 TEXT NOT NULL,
        log_detail               TEXT NOT NULL
    );
    
    """)
    conn.commit()
    conn.close()


def insert_data_source():
    """Insert Data Sources into sqlite database table datasource"""
    db_path = Path(DATABASE_FILE_PATH)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    current_date = datetime.now().date()
    try:
        data = [
            ("Chicken Rates Pipeline", "chicken_rate_pipeline.py", 4, 0.25, current_date),
            ("Gold Silver Rates Pipeline", "gold_rate_pipeline.py", 15, 0.28, current_date),
            ("Al-Fatah Supermarket", "alfatah.py", 30000, 0.4, current_date),
            ("Bata", "bata.py", 800, 0.125, current_date),
            ("Books", "books.py", 320, 0.125, current_date),
            ("Carrefour", "carrefour.py", 8000, 0.375, current_date),
            ("Cars", "cars.py", 5, 0.2, current_date),
            ("Chicken Rates", "chicken_rate.py", 4, 0.25, current_date),
            ("Clothing & Apparels", "clothing.py", 50, 0.2, current_date),
            ("Foodpanda", "foodpanda.py", 16000, 0.32, current_date),
            ("Gold Silver Rates", "gold.py", 15, 0.28, current_date),
            ("Imtiaz Supermarket - I", "imtiaz0.py", 18000, 0.25, current_date),
            ("Imtiaz Supermarket - II", "imtiaz1.py", 17000, 0.3, current_date),
            ("Men Tailors", "men_tailoring.py", 4, 0.25, current_date),
            ("Metro Supermarket", "metro.py", 5000, 0.2, current_date),
            ("Naheed Supermarket", "naheed.py", 18000, 0.25, current_date),
            ("Mobile Networks", "networks.py", 3, 0.67, current_date),
            ("Seafood - Fishes", "other_products.py", 40, 0.25, current_date),
            ("Restaurants", "restaurants.py", 1000, 0.25, current_date),
            ("Sabzi Market Products", "sabzi_market.py", 850, 0.2, current_date),
            ("Airline Tickets", "sastaticket.py", 100, 0.2, current_date),
            ("School Uniforms", "school_uniform.py", 10, 0.2, current_date),
            ("Servis Shoes", "servis.py", 750, 0.25, current_date),
            ("Vegetables & Fruits", "vegetables&fruits.py", 60, 0.17, current_date),
            ("Laundry", "washing.py", 80, 0.25, current_date),
            ("Real Estate", "zameen.py", 12000, 0.33, current_date),
        ]

        cur.executemany("""
            INSERT INTO datasource (datasource_name, datasource_script_name, avg_rows, allowed_deviation, eff_start_dt)
            VALUES (?, ?, ?, ?, ?)
        """, data)

        conn.commit()
        print('Successfully inserted data in table.')
    except Exception as e:
        print("Error in inserting rows into table:\n", e)
    finally:
        conn.close()
    

def get_connection():
    db_path = Path(DATABASE_FILE_PATH)
    conn = sqlite3.connect(db_path, timeout=int(SQLITE_TIMEOUT_SECONDS))
    conn.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE};")
    conn.execute(f"PRAGMA synchronous={SQLITE_SYNCHRONOUS_MODE};")
    return conn


def get_datasource_id(datasource_script_name: str) -> int:
    """
    Fetches and returns the datasource ID for the provided script.
    """
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()

    cur.execute(f"SELECT datasource_id FROM datasource WHERE datasource_script_name = ?;", (datasource_script_name,))
    row = cur.fetchone()
    
    if row is None:
        conn.close()
        raise ValueError(f"No datasource entry found for datasource_script_name = '{datasource_script_name}'")
    
    datasource_id = row[0]

    conn.commit()
    conn.close()
    return datasource_id


def update_status_info(path: str, status: str, 
                    period: date, scraped_rows: int = None,
                    start_time: datetime = None, end_time: datetime = None,
                    duration_seconds: float = None,
                    status_id: int = None) -> int:
    """
    Updates the existing row with relevant status and metrics and returns the row ID in either case.
    """
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()
    
    datasource_script_name = Path(path).name
    if datasource_script_name != METADATA_SCRIPT_NAME:
        datasource_id = get_datasource_id(datasource_script_name)
    else:
        if not status_id:
            datasource_id = METADATA_SOURCE_ID
            cur.execute("""INSERT INTO status
                    (datasource_id, status, period)
                    VALUES (?, ?, ?);"""
                            , (datasource_id, "Pending", period))
            conn.commit()
        

    if not status_id:
        # Fetch status_id
        cur.execute("""
                SELECT MAX(status_id) FROM status
                WHERE
                    datasource_id = ? AND
                    period = ? AND
                    status IN ('Pending', 'Scheduled');
            """, (datasource_id, period))
        row = cur.fetchone()
            
        if row is None:
            conn.close()
            raise ValueError(f"Unable to find status_id for record having datasource_id: {datasource_id}, period: {period}, start_time: {start_time}")
        
        status_id = row[0]

    # Check to update the relevant metrics for a task after it has been completed else it updates
    # only the state of the task to 'Running'
    if scraped_rows is None and end_time is None and duration_seconds is None:
        cur.execute("""
            UPDATE status
            SET
                status = ?,
                start_time = ?
            WHERE
                status_id = ? AND
                status in ('Pending', 'Scheduled');
        """, (status, start_time, status_id))
    else:
        cur.execute("""
            UPDATE status
            SET
                status = ?,
                scraped_rows = ?,
                end_time = ?,
                duration_seconds = ?
            WHERE
                status_id = ? AND
                status = 'Running';
        """, (status, scraped_rows, end_time, duration_seconds, status_id))
        
    conn.commit()
    conn.close()
    return status_id



def insert_scraping_logs(path: str,
                         status_id: int,
                         timestamp: datetime,
                         log_type: str,
                         log_detail: str):
    """
    Inserts one row into logs table.
    """
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()

    datasource_script_name = Path(path).name
    if datasource_script_name != METADATA_SCRIPT_NAME:
        datasource_id = get_datasource_id(datasource_script_name)
    else:
        datasource_id = METADATA_SOURCE_ID

    cur.execute("""
        INSERT INTO logs
          (datasource_id, status_id, timestamp, log_type, log_detail)
        VALUES (?, ?, ?, ?, ?);
    """, (datasource_id, status_id, timestamp, log_type, log_detail))
    conn.commit()
    conn.close()


def reset_status_pending(status_id: int):
    """
    Resets the status to 'Pending' of a failed task ready to be retried
    """
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()
    cur.execute("""
            UPDATE status
            SET
                status = 'Pending'
            WHERE
                status_id = ? AND
                status = 'Running';
        """, (status_id,))
    conn.commit()
    conn.close()


def fetch_avg_rows_and_deviation(path: str):
    """
    Fetches Average Rows and Allowed Deviation for a given data source.
    """
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()
    
    datasource_script_name = Path(path).name
    cur.execute(f"SELECT avg_rows, allowed_deviation FROM datasource WHERE datasource_script_name = ?;"
                , (datasource_script_name,))
    row = cur.fetchone()
    
    if row is None:
        conn.close()
        raise ValueError(f"No datasource entry found for datasource_script_name = '{datasource_script_name}'")
    
    avg_rows, allowed_dev = row[0], row[1]

    conn.close()
    return avg_rows, allowed_dev


def get_pending_datasource() -> list[str]:
    """
    Returns the current list of all pending active data sources
    """
    current_date = datetime.now(tz=pytz.timezone('Asia/Karachi')).date()
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT datasource_script_name FROM datasource ds 
                    INNER JOIN status si ON ds.datasource_id = si.datasource_id
                    WHERE si.status = 'Pending' AND si.period = ? 
                    AND ds.eff_end_dt IS NULL AND si.status_id = 
                        (SELECT MAX(status_id) FROM status WHERE datasource_id = ds.datasource_id);""", (current_date,))
    raw_results = cur.fetchall()
    if raw_results:
        results = [row[0] for row in raw_results]
    else:
        results = []
    
    conn.close()
    return results


def get_scheduled_datasource() -> list[str]:
    """
    Returns the current list of all scheduled active data sources
    """
    current_date = datetime.now(tz=pytz.timezone('Asia/Karachi')).date()
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT datasource_script_name FROM datasource ds 
                    INNER JOIN status si ON ds.datasource_id = si.datasource_id
                    WHERE si.status = 'Scheduled' AND si.period = ? 
                    AND ds.eff_end_dt IS NULL AND si.status_id = 
                        (SELECT MAX(status_id) FROM status WHERE datasource_id = ds.datasource_id);""", (current_date,))
    raw_results = cur.fetchall()
    if raw_results:
        results = [row[0] for row in raw_results]
    else:
        results = []
    
    conn.close()
    return results


def set_sources_pending(sources: list[dict]):
    """
    Marks the status 'Pending' of only those sources provided in the list as argument.
    """
    current_date = datetime.now(tz=pytz.timezone('Asia/Karachi')).date()
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()
    for source in sources:
        status_id = int(source["status_id"])
        datasource_id = int(source["datasource_id"])
        status = str(source["status"]).capitalize()
        period = datetime.strptime(source["period"], "%Y-%m-%d").date()
        if status != "Running":
            # Insert a new row marking the provided sources as pending
            cur.execute("""INSERT INTO status
            (datasource_id, status, period)
            VALUES (?, ?, ?);"""
                    , (datasource_id, "Pending", current_date))
    
    conn.commit()
    conn.close()


def schedule_sources(datasources: list[int] = None):
    """
    Inserts and marks the status of all the active data sources as 'Scheduled' in status table.
    Optionally inserts and marks the status of provided data sources as 'Scheduled' in status table.
    """
    current_date = datetime.now(tz=pytz.timezone('Asia/Karachi')).date()
    try:
        conn = get_connection()
    except Exception as e:
        print("Couldn't connect to the SQLite Database.")
        raise
    cur = conn.cursor()
    if datasources:
        for datasource_id in datasources:
            # Insert a new row for each new data source provided marking it as scheduled
            cur.execute("""INSERT INTO status
                        (datasource_id, status, period)
                        VALUES (?, ?, ?);"""
                        , (datasource_id, "Scheduled", current_date))
    else:
        # Insert a new row for each active data source marking it as pending (Note: period is set to tomorrow based on scheduling time of 12:00AM - period(?, '+1 day'))
        cur.execute("""INSERT INTO status
                (datasource_id, status, period)
                SELECT datasource_id, 'Scheduled', ?
                FROM datasource
                WHERE eff_end_dt is NULL;""", (current_date,))
    
    conn.commit()
    conn.close()


if __name__ == "__main__":
    initialize_db()
    print(f"Initialized database successfully.")
    insert_data_source()