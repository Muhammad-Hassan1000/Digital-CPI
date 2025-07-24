import os
# os.environ["PREFECT_HOME"] = "/.prefect"
# os.environ["PREFECT_API_URL"] = "http://127.0.0.1:8500/api"
# os.environ["PREFECT_UI_URL"] = "http://127.0.0.1:8500"
# os.environ["PREFECT_API_DATABASE_CONNECTION_URL"] = "sqlite+aiosqlite:///D:/hassan/SBP Work/Digital CPI/Work/Scraping/Final Solution/.prefect/prefect.db?mode=wal&cache=shared"
import dotenv

dotenv.load_dotenv()


TASK_CONCURRENCY_LIMIT = int(os.getenv("TASK_CONCURRENCY_LIMIT"))
METADATA_SCRIPT_PATH = os.getenv("METADATA_SCRIPT_PATH")
METADATA_SCRIPT_NAME = os.getenv("METADATA_SCRIPT_NAME")
DATA_DIR = os.getenv("DATA_DIR")
SCRIPTS_DIR = os.getenv("SCRIPTS_DIR")

import subprocess
import asyncio
import sys
import json
from datetime import datetime, timedelta, date
import pytz
from pathlib import Path
import pandas as pd
from prefect import flow, task, get_run_logger, get_client, schedules
from prefect.runtime import task_run

from logger import get_logger
from db_helper import get_pending_datasource, get_scheduled_datasource, update_status_info, insert_scraping_logs, fetch_avg_rows_and_deviation, reset_status_pending


def parse_args():
    """
    Parse command-line argument. Expects one positional argument:
      The nature of pipeline to run, e.g.
      'adhoc' or 'scheduled'
    """
    nature = sys.argv[1]
    print(f"Starting Prefect pipeline with nature: {nature}")
    return nature



@task(task_run_name="Database-Insertion-Task-for-{task}", log_prints=True)
def db_insertion_task(result_dict: dict, task: str, file_logger):
    prefect_logger = get_run_logger()

    # for result_dict in results:
    status_id = result_dict["status_id"]
    path = result_dict["path"]
    status = result_dict["status"]
    date = result_dict["date"]
    scraped_rows = result_dict["scraped_rows"]
    start_time = result_dict["start_time"]
    end_time = result_dict["end_time"]
    duration = result_dict["duration_seconds"]
    output = result_dict["output"] # List of dictionaries

    file_logger.info(f"Starting database insertion for {path} with status {status}")
    prefect_logger.info(f"Passing the following parameters to insert into status table: [{path}, {status}, {date}, {scraped_rows}, {start_time}, {end_time}, {duration}, {status_id}]")

    try:
        # Upsert into Status, get back status_id
        status_id = update_status_info(
            status_id=status_id,
            path=path,
            status=status,
            period=date,
            scraped_rows=scraped_rows,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration
        )
        file_logger.info(f"Successfully inserted status into status table with status_id: {status_id}")
        prefect_logger.info(f"✅ Successfully inserted status into status table with status_id: {status_id}")
    except Exception as e:
        file_logger.error(f"Failed to insert status into database: {e}")
        prefect_logger.exception(f"💥 Exception occured while inserting in status table with error: {e}")

    # Iterate to insert logs for each task/script
    for log_dict in output:
        file_logger.debug(f"Inserting log entry for {path}: {log_dict['log_type']} - {log_dict['line'][:100]}...")
        prefect_logger.info(f"Passing the following parameters to insert into scraping_logs table: [{path}, {status_id}, {log_dict["timestamp"]}, {log_dict["log_type"]}, {log_dict["line"]}]")
        try:
            # Insert into scraping_logs
            insert_scraping_logs(
                path=path,
                status_id=status_id,
                timestamp=log_dict["timestamp"],
                log_type=log_dict["log_type"],
                log_detail=log_dict["line"]
            )
            file_logger.debug(f"Successfully inserted log entry into scraping_logs table")
            prefect_logger.info(f"✅ Successfully inserted logs into scraping_logs table.")
        except Exception as e:
            file_logger.error(f"Failed to insert log entry: {e}")
            prefect_logger.exception(f"💥 Exception occured while inserting in scraping_logs table with error: {e}")

    file_logger.info(f"Completed database operations for {path}")
    prefect_logger.info(f"📄 Logged '{path}' (status={status}) into DB (status_id={status_id}).")


def task_retry_handler(output_lines: list[str], exception: Exception | None) -> bool:
    """
    Decide whether the failure (based on output lines or exception) is transient.
    Return True to signal a retry, False to treat as non-transient.
    """
    # Define substrings or regex patterns that indicate transient conditions.
    transient_indicators = [
        "Transient subprocess failure", # Handle error raised by this function
        "Threshold not met",            # Rows not scraped correctly
        # "Database is locked",
        "Timeout", "timed out", 
        "Connection refused", "Connection Failed", 
        "Connection reset", "ERR_TUNNEL_CONNECTION_FAILED",
        "Temporary failure", "temporarily unavailable",
        "503", "502", "504",        # HTTP server error codes
        "DNS", "Name or service not known",
        "socket.timeout",           # Python socket timeout
        "TimeoutError",             # generic
        "SSLError",                 # SSL handshake could be transient
        "SessionNotCreated"         # Chromedriver exception
    ]
    # First check exception type/message if provided
    if exception is not None:
        msg = str(exception)
        for indicator in transient_indicators:
            if indicator.lower() in msg.lower():
                return True

    # Next inspect subprocess output lines
    for line_dict in output_lines:
        line = line_dict["line"]
        for indicator in transient_indicators:
            if indicator.lower() in line.lower():
                return True

    return False


def group_multi_line_errors(stream):
    """
    Yield blocks from the stream. Non-traceback lines are yielded individually;
    when a line contains 'Traceback' or 'Stacktrace', we start buffering,
    and continue appending any subsequent lines that start with whitespace.
    Once we hit a non-indented line (and we are in a traceback), we flush the traceback block
    and treat that non-indented line as a new (separate) block.
    """
    buffering = False
    buffer = ""
    for line in stream:
        # If this line is “empty” (only whitespace/newline)
        if line.strip() == "":
            if buffering:
                # Inside a traceback: keep blank lines as part of it
                buffer += line
            else:
                # Outside traceback: skip entirely (do not yield a separate empty block)
                continue
            # Then go to next line
            continue

        if "Traceback" in line or "Stacktrace" in line or "Error" in line:
            # Start of a traceback block
            if buffer:
                yield buffer
            buffering = True
            buffer = line
        elif buffering:
            # We are inside a traceback: continuation lines are indented,
            # or sometimes the final exception line may not be indented but we still
            # want to keep it as part of the traceback.
            if line.startswith((' ', '\t', '#')):
                buffer += line
            else:
                # The new line is not indented: likely end of traceback
                # Flush the buffered traceback
                yield buffer
                buffering = False
                buffer = line
        else:
            # Not in traceback mode, yield line-by-line
            if buffer:
                yield buffer
            buffer = line
    # After loop, flush anything left
    if buffer:
        yield buffer



def list_scripts(directory: str, sources_to_scrape: list[str]) -> list[str]:
    """
    Creates and returns the relative path of all the pending scripts to execute 

    Arguments:
    - directory: path to the directory containing .py scripts
    - sources_to_scrape: list of filenames (e.g. ['gold.py']) to execute
    """
    return [
        os.path.join(directory, file)
        for file in os.listdir(directory)
        if file in sources_to_scrape
    ]


# Function to timestamp each line of subprocess for logging purpose
def timestamped(line: str, log_type: str) -> dict:
    now = datetime.strptime(datetime.now(tz=pytz.timezone('Asia/Karachi')).replace(tzinfo=None).isoformat(sep="_", timespec="seconds"), "%Y-%m-%d_%H:%M:%S")
    return {"timestamp": now, "log_type": log_type, "line": line.rstrip("\n")}


# Function to tag the logs generated
def tag_logs(block):
    # Determine log type: if it’s a traceback, treat as ERROR
    if "Stacktrace" in block or "CHROME" in block.upper() or "PLAUSIBLE" in block.upper() or "SELENIUM" in block.upper():
        log_type = "SELENIUM LOG"
    elif "Traceback" in block or "Exception" in block or "ERROR" in block.upper():
        log_type = "ERROR"
    else:
        log_type = "OUTPUT"
    return log_type


def get_rows_scraped(py_path: str, date: date) -> int:
    file_name = Path(py_path).stem
    csv_path = os.path.join(DATA_DIR, date.isoformat(), file_name + ".csv")
    if os.path.exists(csv_path):
        file_logger.debug(f"Reading output file from path: {csv_path}")
        df = pd.read_csv(csv_path)
        total_rows = df.shape[0]
        return total_rows
    
    raise FileNotFoundError(f"No CSV file found for script {file_name} at path: {csv_path}")


def get_final_state(scraped_rows: int, avg_rows: int, allowed_dev: float):
    upper_bound = round(avg_rows * (1 + allowed_dev))
    lower_bound = round(avg_rows * (1 - allowed_dev))
    file_logger.info(f"Total rows scraped: {scraped_rows}, Upper bound for file: {upper_bound}, Lower bound for file: {lower_bound}")
    if scraped_rows >= lower_bound and scraped_rows <= upper_bound:
        final_status = "Completed"
    else:
        final_status = "Failed"
    return lower_bound, upper_bound, final_status
    

def handle_failed_task_files(path: str, date: date):
    file_name = Path(path).stem
    csv_path = os.path.join(DATA_DIR, date.isoformat(), file_name + ".csv")
    if os.path.exists(csv_path):
        file_logger.debug(f"Deleting following output file for Failed task: {csv_path}")
        # new_csv_name = Path(csv_path).stem + "-1" + Path(csv_path).suffix
        # os.rename(csv_path, new_csv_name)
        os.remove(csv_path)
    
    print(f"No CSV file found for script {file_name} at path: {csv_path}")


def rename_old_files(data_path: str, script_name: str):
    file_path = os.path.join(data_path, f"{script_name}.csv")
    print(f"Searching if following file exists: {file_path}")
    if os.path.exists(file_path):
        timestamp = os.path.getmtime(file_path)
        formatted_ts = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d_%H-%M-%S')
        new_file_path = f"{os.path.splitext(file_path)[0]}_{formatted_ts}.csv"
        print(f"Renaming the existing file to: {new_file_path}")
        os.rename(file_path, new_file_path)


@task(task_run_name="{path}", retries=2, retry_delay_seconds=[30, 60], tags=["scraping"], persist_result=True, log_prints=True)
def run_script(path: str):
    """
    Execute a Python script via subprocess and return a dictionary with metadata.
    Raises if the script returns a nonzero exit code.
    """

    # Variable Initialization
    full_stdout_stderr = []    # List to accumulate full stdout/stderr
    scraped_rows = 0
    status_id = None
    
    prefect_logger = get_run_logger()

    task_run_count = task_run.get_run_count()
    max_retries = run_script.retries
    
    date = datetime.now(tz=pytz.timezone('Asia/Karachi')).date()
    
    prefect_logger.info(f"▶️ Starting {Path(path).name}")
    start_time = datetime.strptime(datetime.now(tz=pytz.timezone('Asia/Karachi')).replace(tzinfo=None).isoformat(sep="_", timespec="seconds"), "%Y-%m-%d_%H:%M:%S")
        

    try:
        try:
            prefect_logger.info("Creating date subfolder if not exists.")
            data_path = os.path.join(DATA_DIR, date.isoformat())
            if not os.path.exists(data_path):
                os.mkdir(data_path)
                
            script_name = Path(path).stem
            prefect_logger.info(f"Handling Old Existing files for script: {script_name}")
            rename_old_files(data_path, script_name)
        except Exception as e:
            Exception(f"Exception encountered while preprocessing for task with exception: {e!r}")

        # Launching the subprocess with Popen to stream its output
        process = subprocess.Popen(
            ["python", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1            # line-buffered
            # universal_newlines=True
        )

        try:
            status_id = update_status_info(
                path=path,
                status="Running",
                period=date,
                start_time=start_time
            )
            if status_id:
                prefect_logger.info(f"Successfully inserted running state of script '{path}' with status id = {status_id}")
            else:
                raise RuntimeError(f"Neither Pending nor Scheduled status found for script {Path(path).name}")
        except Exception as e:
            # if task_retry_handler(None, e):
            #     prefect_logger.warning(f"Database locked. Will retry the task.")
            # else:
            raise Exception(f"Failed to insert running state data due to exception: {e!r}")

        for block in group_multi_line_errors(process.stdout):
            log_type = tag_logs(block)
            line_ts = timestamped(block, log_type)
            prefect_logger.info(f"{line_ts['timestamp']} {line_ts['log_type']} -\n{line_ts['line'].rstrip()}")
            full_stdout_stderr.append(line_ts)

        returncode = process.wait()
        end_time = datetime.strptime(datetime.now(tz=pytz.timezone('Asia/Karachi')).replace(tzinfo=None).isoformat(sep="_", timespec="seconds"), "%Y-%m-%d_%H:%M:%S")
        duration = (end_time - start_time).total_seconds()


        if returncode is not None and returncode != 0:
            prefect_logger.info(f"📄 Output from subprocess: {full_stdout_stderr}")

            handle_failed_task_files(path, date)

            # Script exited with non-zero. Inspect output lines for transient patterns.
            if task_retry_handler(full_stdout_stderr, None):
                prefect_logger.warning(f"Transient failure detected in {path} (exit code {returncode}).")
                # Check if it is the last retry
                if task_run_count <= max_retries:
                    # Re-mark the status of this source as Pending
                    prefect_logger.info(f"Resetting status to Pending for the task to be retried with status id: {status_id}")
                    reset_status_pending(status_id)
                    # Raise to trigger retry
                    raise RuntimeError(f"Transient subprocess failure, exit code {returncode}. Will retry.")
                else:
                    log_message = f"Error - Maximum retries exhausted."
                    prefect_logger.error("❌ " + log_message)

                    log_type = tag_logs(log_message)
                    line_ts = timestamped(log_message, log_type)
                    full_stdout_stderr.append(line_ts)
                    status = "Failed"
                    results = {
                        "status_id": status_id,
                        "path": path,
                        "date": date,
                        "scraped_rows": scraped_rows,
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration_seconds": duration,
                        "status": status,
                        "output": full_stdout_stderr
                    }
                    db_insertion_task(result_dict=results, task=Path(path).stem, file_logger=file_logger)
                    

            else:
                # Non-transient failure: log and proceed without retry
                log_message = f"Error - {Path(path).name} exited with code {returncode}, marking as final failure (no retry)."
                prefect_logger.error("❌ " + log_message)
                
                log_type = tag_logs(log_message)
                line_ts = timestamped(log_message, log_type)
                full_stdout_stderr.append(line_ts)
                status = "Failed"
                results = {
                    "status_id": status_id,
                    "path": path,
                    "date": date,
                    "scraped_rows": scraped_rows,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": duration,
                    "status": status,
                    "output": full_stdout_stderr
                }
                db_insertion_task(result_dict=results, task=Path(path).stem, file_logger=file_logger)
                

        # If process is successful, finalize its status based on business threshold
        if returncode == 0:
            if Path(path).name != METADATA_SCRIPT_NAME:
                scraped_rows = get_rows_scraped(path, date)
                avg_rows, allowed_dev = fetch_avg_rows_and_deviation(path)
                lower_bound, upper_bound, status = get_final_state(scraped_rows, avg_rows, allowed_dev)

                log_message = f"Total rows scraped: {scraped_rows}, Lower bound for file: {lower_bound}, Upper bound for file: {upper_bound}"
                prefect_logger.info(log_message)

                log_type = tag_logs(log_message)
                line_ts = timestamped(log_message, log_type)
                full_stdout_stderr.append(line_ts)
            else:
                status = "Completed"

            if status == "Failed":
                log_message = f"Error - {Path(path).name} succeeded but marked failed at {end_time} due to failing the threshold of scraping."
                prefect_logger.error("❌ " + log_message)

                log_type = tag_logs(log_message)
                line_ts = timestamped(log_message, log_type)
                full_stdout_stderr.append(line_ts)
                
                handle_failed_task_files(path, date)
                
                # Check if it is the last retry
                if task_run_count <= max_retries:
                    # Re-mark the status of this source as Pending
                    prefect_logger.info(f"Resetting status to Pending for the task to be retried with status id: {status_id}")
                    reset_status_pending(status_id)
                    # Raise to trigger retry
                    raise RuntimeError(f"Defined scraping threshold not met. Retrying Task.")
                else:
                    log_message = f"Error - Maximum retries exhausted."
                    prefect_logger.error("❌ " + log_message)

                    log_type = tag_logs(log_message)
                    line_ts = timestamped(log_message, log_type)
                    full_stdout_stderr.append(line_ts)
                    status = "Failed"
                    results = {
                        "status_id": status_id,
                        "path": path,
                        "date": date,
                        "scraped_rows": scraped_rows,
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration_seconds": duration,
                        "status": status,
                        "output": full_stdout_stderr
                    }
                    db_insertion_task(result_dict=results, task=Path(path).stem, file_logger=file_logger)
                    

            else:
                log_message = f"{Path(path).name} succeeded at {end_time}"
                prefect_logger.info("✅ " + log_message)
                log_type = tag_logs(log_message)
                line_ts = timestamped(log_message, log_type)
                full_stdout_stderr.append(line_ts)

            results = {
                "status_id": status_id,
                "path": path,
                "date": date,
                "scraped_rows": scraped_rows,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "status": status,
                "output": full_stdout_stderr
            }
            db_insertion_task(result_dict=results, task=Path(path).stem, file_logger=file_logger)
            

    except Exception as exc:
        end_time = datetime.strptime(datetime.now(tz=pytz.timezone('Asia/Karachi')).replace(tzinfo=None).isoformat(sep="_", timespec="seconds"), "%Y-%m-%d_%H:%M:%S")
        duration = (end_time - start_time).total_seconds()
        
        if task_retry_handler([], exc):
            log_message = f"Transient failure detected while processing {path}: {exc!r}. Will retry."
            prefect_logger.warning(log_message)

            log_type = tag_logs(log_message)
            line_ts = timestamped(log_message, log_type)
            full_stdout_stderr.append(line_ts)
            results = {
                "status_id": status_id,
                "path": path,
                "date": date,
                "scraped_rows": scraped_rows,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "status": "Failed",
                "output": full_stdout_stderr
            }
            db_insertion_task(result_dict=results, task=Path(path).stem, file_logger=file_logger)
            raise
        else:
            log_message = f"Exception detected while processing {path}: {exc!r}. Marking as Failed."
            prefect_logger.exception("💥 " + log_message)  
            log_type = tag_logs(log_message)
            line_ts = timestamped(log_message, log_type)
            full_stdout_stderr.append(line_ts)
            # full_stdout_stderr.extend([{'timestamp': end_time, 'log_type': 'ERROR', 'line': str(exc)}])
            results = {
                "status_id": status_id,
                "path": path,
                "date": date,
                "scraped_rows": scraped_rows,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "status": "Failed",
                "output": full_stdout_stderr
            }
            db_insertion_task(result_dict=results, task=Path(path).stem, file_logger=file_logger)
    

# Function to setup Task concurrency limits
async def set_concurrency_limits():
    async with get_client() as client:
        limit_id = await client.create_concurrency_limit(tag="scraping", concurrency_limit=TASK_CONCURRENCY_LIMIT)
        get_run_logger().info(f"Configured 'scraping' concurrency limit = {TASK_CONCURRENCY_LIMIT}")



@flow(flow_run_name="Price-Ingestion-Parallel", retries=1, retry_delay_seconds=20, log_prints=True)
async def master_scraping_pipeline(nature: str, directory: str = SCRIPTS_DIR):
    """
    1. Discover all active python scripts
    2. Launch each as a separate Prefect task in parallel
    3. Collect and log their outputs
    """
    # Instantiate custom file-based logger for other logging
    global file_logger
    file_logger = get_logger()

    # Instantiate Prefect's built-in logger for its own logging
    prefect_logger = get_run_logger()

    await set_concurrency_limits()

    if nature == "scheduled":
        sources_to_scrape = get_scheduled_datasource()
        prefect_logger.info(f"Scheduled and active Datasources: {sources_to_scrape}")
    elif nature == "adhoc":
        sources_to_scrape = get_pending_datasource()
        prefect_logger.info(f"Pending and active Datasources: {sources_to_scrape}")

    scripts = list_scripts(directory, sources_to_scrape)
    prefect_logger.info(f'Files to execute: {scripts}')
    
    # Map run_script across all found scripts => parallel execution
    futures = run_script.map(scripts)

    futures.wait()

    # completed_results = [task_future.result() for task_future in futures]

    # file_logger.info(f"Starting DB Insertion Task for Scraping Task results")
    # # Spinning off db_insertion_task in parallel and independently to avoid breaking business flow
    # db_handle = db_insertion_task.submit(completed_results, file_logger)

    # Executing data_files_analysis.py file to collect meta info of scraped data files
    script = METADATA_SCRIPT_PATH
    prefect_logger.info(f'File to execute: {script}')
    result = run_script(script)

    # try:
    #     db_handle.result()
    # except NameError as e:
    #     pass
    
    # file_logger.info(f"Starting DB Insertion Task for Metadata Script results")
    # # Spinning off another db_insertion_task in parallel and independently to avoid breaking business flow
    # db_handle_2 = db_insertion_task.submit([meta_results], file_logger)

    # db_handle_2.result()


if __name__ == "__main__":
    nature = sys.argv[1]
    print(f"Starting Prefect pipeline with nature: {nature}")
    try:
        asyncio.run(master_scraping_pipeline(nature=nature))
        # asyncio.run(master_scraping_pipeline.serve(name="Daily-Scraper", schedule=schedules.Schedule(cron="35 15 * * *", timezone="Asia/Karachi")))
        print(f"Successfully started Master Scraping Pipeline.")
    except Exception as e:
        raise Exception(f"Exception encountered while trying to start Master Scraping Pipeline: {e!r}")
    
