import os
import dotenv

dotenv.load_dotenv()

PIPELINE_SCRIPT_PATH = os.getenv("PIPELINE_SCRIPT_PATH")

import subprocess
import sys
import json
from logger import get_logger
from db_helper import set_sources_pending, schedule_sources


def parse_args():
    """
    Parse command-line arguments. Expects one positional argument:
      a JSON string, e.g.
      '{"data": [{"status_id": 13, "datasource_id": 8, "period": "2025-06-23", "status": "pending"}, ...]}'
    """
    try:
        payload = sys.argv[1]
    except IndexError:
        return None
    try:
        parsed_json = json.loads(payload)
        return parsed_json
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e.msg}") from e
    
    

def main():
    """
    Load an optional payload as JSON. Marks the source as pending for execution or schedules for next run
    if datasource parameters/ script has been modified, based on the payload structure.
    Otherwise, invoked by cronjob without any parameters, in which case all active datasources are
    scheduled for execution.
    """
    logger = get_logger()

    try:
        parsed_json = parse_args()
        
        if parsed_json:
            data_list = parsed_json.get("data")
            if data_list is None:
                raise ValueError("JSON payload must contain a 'data' key.")
            if not isinstance(data_list, list):
                raise ValueError("The 'data' field must be a list.")
            
            source_ids = []     # Holds existing datasource ids, that only need to be re-run
            for item in data_list:
                source_id = item["datasource_id"]
                source_id = int(source_id)
                status = item["status"]
                status = str(status).capitalize()
                if status != "Running":
                    source_ids.append(source_id)
                else:
                    logger.exception("Cannot run script which is already in Running state!")
                    raise RuntimeError("Cannot run script which is already in Running state!")

            if source_ids:
                logger.info(f"Source ids to mark for execution: {source_ids}")
                try:
                    logger.debug(f"JSON being passed to update status: {data_list}")
                    set_sources_pending(sources=data_list)
                    logger.info("Successfully updated the status of requested source IDs.")
                    try:
                        subprocess.Popen(["python", PIPELINE_SCRIPT_PATH, "adhoc"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        logger.info("Pipeline invoked successfully")
                    except Exception as e:
                        logger.error(f"Exception encountered while invoking prefect pipeline: {e!r}")
                except Exception as e:
                    logger.error(f"Exception encountered while marking status Pending: {e!r}")
        
        else:
            try:
                logger.info("No source ids provided, defaulting to mark all active sources as scheduled.")
                schedule_sources()
                logger.info("Successfully updated the status of all active datasource IDs.")
                try:
                    subprocess.Popen(["python", PIPELINE_SCRIPT_PATH, "scheduled"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    logger.info("Pipeline invoked successfully")
                except Exception as e:
                    logger.error(f"Exception encountered while invoking prefect pipeline: {e!r}")
            except Exception as e:
                logger.error(f"Exception encountered while marking status Pending: {e!r}")
        
    except Exception as e:
        logger.error(f"Request failed with error: {e!r}")
        

    

if __name__ == "__main__":
    main()