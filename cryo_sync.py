#!/usr/bin/env python3
import os
import time
import json
import logging
import subprocess
import argparse
from multiprocessing import Process

ASCII_HEADER = r"""
 _                                    _ _           ____                  _ 
| |    ___  _ __   __ _  __ _  __   _(_) |_ __ _   / ___|_ __ _   _  ___ | |
| |   / _ \| '_ \ / _` |/ _` | \ \ / / | __/ _` | | |   | '__| | | |/ _ \| |
| |__| (_) | | | | (_| | (_| |  \ V /| | || (_| | | |___| |  | |_| | (_) |_|
|_____\___/|_| |_|\__, |\__,_|   \_/ |_|\__\__,_|  \____|_|   \__, |\___/(_)
                  |___/                                       |___/         

Author: the Magellan team
Description: Monitors project directories for new files and transfers them
             via rsync if a threshold is reached. Projects marked as
             "detached" in the project.json file are skipped.

Function Call Flow:
1. monitor_projects() - Main loop to check project directories.
2. should_scan_project() - Determines if a project should be scanned.
3. get_new_files_count() - Counts new files in the project directory.
4. run_rsync() - Executes the rsync command to transfer files.
5. process_project() - Manages the transfer process and retries.
"""

# Configuration
BASE_DIR = 'CryoSparc/'  # Base directory where project folders are located
DESTINATION = 'copied_Cryosparc/'  # Remote destination for rsync transfer
THRESHOLD = 5  # Number of new files required to trigger an rsync transfer
RETRY_LIMIT = 3  # Number of retries if rsync fails
CHECK_INTERVAL = 10  # Time interval (in seconds) between scans of the project directories
PROJECT_JSON_FILE = 'project.json'  # JSON file used to determine if a project is marked as "detached"


# Logging setup
logging.basicConfig(
    filename='rsync_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_new_files_count(project_dir):
    """Get the count of new files in the specified project directory."""
    try:
        with os.scandir(project_dir) as it:
            return sum(1 for entry in it if entry.is_file())
    except Exception as e:
        logging.error(f'Error accessing {project_dir}: {e}')
        return 0

def run_rsync(project_dir):
    """Run the rsync command for the specified project directory."""
    command = ["rsync", "-avz", "--relative", project_dir + "/", DESTINATION]
    try:
        logging.info(f'Starting rsync from {project_dir}')
        result = subprocess.run(command, check=False)
        return result.returncode
    except Exception as e:
        logging.error(f'Error running rsync: {e}')
        return 1

def is_project_detached(project_dir):
    """Check if the project is marked as detached based on the project.json file."""
    json_path = os.path.join(project_dir, PROJECT_JSON_FILE)
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
                return data.get("detached", False)
        except Exception as e:
            logging.error(f'Error reading {json_path}: {e}')
    return False

def should_scan_project(project_dir):
    """Check if the project directory should be scanned."""
    return not is_project_detached(project_dir)

def monitor_projects(verbose=False):
    """Monitor all project directories and trigger rsync as needed."""
    while True:
        if verbose:
            logging.info("Starting project monitoring loop...")

        project_dirs = [
            os.path.join(BASE_DIR, d)
            for d in os.listdir(BASE_DIR)
            if os.path.isdir(os.path.join(BASE_DIR, d)) and
            should_scan_project(os.path.join(BASE_DIR, d))
        ]

        if verbose:
            logging.info(f"Found project directories: {project_dirs}")

        for project_dir in project_dirs:
            current_count = get_new_files_count(project_dir)
            if verbose:
                logging.info(f"Checking project '{project_dir}': found {current_count} new files.")

            if current_count >= THRESHOLD:
                logging.info(f'New files in {project_dir}, triggering rsync...')
                p = Process(target=process_project, args=(project_dir,))
                p.start()
            elif verbose:
                logging.info(f'Not enough new files in {project_dir} (threshold: {THRESHOLD}, found: {current_count})')

        time.sleep(CHECK_INTERVAL)

def process_project(project_dir):
    """Process a project directory to run rsync."""
    retries = 0
    while retries < RETRY_LIMIT:
        if run_rsync(project_dir) == 0:
            logging.info(f'Rsync completed for {project_dir}')
            break
        retries += 1
        logging.warning(f'Rsync failed for {project_dir}. Retrying... ({retries}/{RETRY_LIMIT})')
        time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor project directories and transfer files using rsync.")
    parser.add_argument('-v', '--verbose', action='store_true', help="Enable verbose logging for troubleshooting.")
    args = parser.parse_args()

    print(ASCII_HEADER)  # Print the ASCII art header
    monitor_projects(args.verbose)

