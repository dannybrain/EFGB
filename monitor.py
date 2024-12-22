#!/usr/bin/env python3
import os
import shutil
import time
import sys
import argparse
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock # Function to sync files using multithreading


def compare_and_get_files_to_sync(source_dir, tracked_files, dest_files, timeout, retries, logger):
    files_to_sync = set()  # Use a set to store files to sync
    source_files = set()   # Set for source files
    dest_files = set(dest_files)  # Set for destination files (already provided as an argument)
    files_processed = 0  # To track the number of files processed

    # Recursively scan the source directory using os.scandir
    def scan_directory(directory):
        nonlocal files_processed
        try:
            logger.info(f"Scanning source directory: {directory} for files...")  # Log the scanning info
            for entry in os.scandir(directory):
                if entry.is_file():
                    file_path = os.path.relpath(entry.path, source_dir)

                    if file_path not in tracked_files and file_path not in dest_files:
                        # New file found, mark for sync
                        files_to_sync.add(file_path)

                    source_files.add(file_path)  # Add to source files set
                    files_processed += 1
                    # Log progress every 5000 files processed
                    if files_processed % 5000 == 0:
                        logger.info(f"Progress: {files_processed} files checked...")
                elif entry.is_dir():
                    # Recursively scan subdirectories
                    scan_directory(entry.path)
        except Exception as e:
            logger.error(f"Error scanning directory {directory}: {e}")

    # Retry logic for checking files
    for attempt in range(1, retries + 1):
        time.sleep(timeout)
        files_to_sync.clear()  # Clear files list before scanning
        source_files.clear()   # Clear source files set
        files_processed = 0  # Reset file counter

        # Scan the source directory for files to sync
        scan_directory(source_dir)
        
        if files_to_sync:
            logger.info(f"Files to sync found: {len(files_to_sync)} new files.")
            break
        else:
            logger.info(f"No new files found. Retrying [{attempt}/{retries}]...")

    return files_to_sync, source_files, dest_files

# Function to setup logging
def setup_logging(dest_dir, project_name, verbose):
    base_dest_dir = "/root/cryosparc/common_workspace"
    if dest_dir:
        dest_dir = os.path.join(base_dest_dir, dest_dir)
    else:
        print("Error: DEST_DIR not provided, can't create log files.")
        sys.exit(1)
        
    dest_dir_project = os.path.join(dest_dir, project_name)
    os.makedirs(dest_dir_project, exist_ok=True)

    logger = logging.getLogger("sync_logger")
    logger.setLevel(logging.DEBUG)

    # Error handler for error_log
    error_log = os.path.join(dest_dir_project, "error_log.txt")
    logger.info(f"Created error_log.txt at {error_log}")
    error_handler = logging.FileHandler(error_log)
    error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    error_handler.setLevel(logging.ERROR)
    logger.addHandler(error_handler)

    # Console handler to show INFO logs
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"))
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    if verbose:
        sync_log = os.path.join(dest_dir_project, "sync_log.txt")    
        logger.info(f"Created sync_log.txt at {sync_log}")
        try:
            file_handler = logging.FileHandler(sync_log)
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to initialize sync_log.txt: {e}")
            logger.info(f"Failed to initialize sync_log.txt: {e}")

    return logger, error_log

# Function to copy a file with retries
def copy_file_with_retries(file_source, file_dest, retries=3, delay=5, logger=None):
    attempt = 0
    while attempt < retries:
        try:
            shutil.copy2(file_source, file_dest)
            return True
        except Exception as e:
            logger.error(f"Error copying {file_source} to {file_dest}: {e}")
            attempt += 1
            if attempt < retries:
                logger.info(f"Retrying {file_source}... Attempt {attempt}/{retries}")
                time.sleep(delay)
            else:
                return False
    return False

# Function to copy a batch of files
def copy_file_batch(files_batch, source_dir, dest_dir, logger, verbose, retries=3):
    results = []
    for file in files_batch:
        file_source = os.path.join(source_dir, file)
        if os.path.islink(file_source):
            logger.debug(f"Skipping symbolic link: {file_source}")
            continue
        file_dest = os.path.join(dest_dir, file)
        os.makedirs(os.path.dirname(file_dest), exist_ok=True)
        success = copy_file_with_retries(file_source, file_dest, retries=retries, logger=logger)
        if success:
            results.append((file_source, file_dest))
            if verbose:
                logger.debug(f"Transferred: {file_source} -> {file_dest}")
        else:
            logger.error(f"Failed to transfer: {file_source} -> {file_dest}")
    return results


# Function to sync files using multithreading
def sync_files(source_dir, dest_dir, files_to_sync, logger, verbose, batch_size=100, retries=3, tracked_files=None):
    os.makedirs(dest_dir, exist_ok=True)
    total_files = len(files_to_sync)
    synced_files_count = 0
    synced_files_lock = Lock()  # Protect shared state with a lock
    failed_files = 0
    
    logger.info(f"Starting synchronization of {total_files} files to {dest_dir}.")
    file_batches = [files_to_sync[i:i + batch_size] for i in range(0, total_files, batch_size)]

    def sync_batch(batch):
        nonlocal failed_files
        try:
            results = copy_file_batch(batch, source_dir, dest_dir, logger, verbose, retries)
            with synced_files_lock:
                nonlocal synced_files_count
                synced_files_count += len(results)
            return results
        except Exception as e:
            logger.error(f"Error syncing batch: {e}")
            with synced_files_lock:
                failed_files += 1
            return []

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(sync_batch, batch) for batch in file_batches]
        for future in as_completed(futures):
            future.result()  # Wait for each batch to complete
        
            # Report progress in a thread-safe manner
            with synced_files_lock:
                current_progress = int((synced_files_count / total_files) * 100)
                logger.info(f"Progress: {current_progress}% , files [{synced_files_count}/{total_files}]")
    
    logger.info(f"Total files synced: {total_files}. Failed files: {failed_files}.")

def monitor_directory(source_dir, tracked_files, timeout, retries, logger, source_files, dest_files):
    if logger:
        logger.info(f"Monitoring directory: {source_dir} for new files...")
    
    new_files = []
    files_processed = 0  # Counter for how many files we've processed

    # Recursively scan the directory using os.scandir
    def scan_directory(directory):
        nonlocal files_processed, new_files
        try:
            for entry in os.scandir(directory):
                if entry.is_file():
                    file_path = os.path.relpath(entry.path, source_dir)
                    if file_path not in tracked_files and file_path not in dest_files:
                        new_files.append(file_path)
                    # Periodic update for every 4,000 files processed
                    files_processed += 1
                    if files_processed % 4000 == 0:
                        logger.info(f"Still processing... Found {files_processed} files so far.")
                elif entry.is_dir():
                    # Recursively scan subdirectories
                    scan_directory(entry.path)
        except Exception as e:
            logger.error(f"Error scanning directory {directory}: {e}")

    # Retry logic for checking files
    for attempt in range(1, retries + 1):
        time.sleep(timeout)
        new_files.clear()  # Clear new files list before scanning
        files_processed = 0  # Reset file counter

        # Scan the source directory for new files
        scan_directory(source_dir)
        
        if new_files:
            logger.info(f"Done! Found {len(new_files)} new files.")
            break
        else:
            logger.info(f"No new files found. Retrying [{attempt}/{retries}]...")

    return new_files

# Update project.json to set "detached" to True and copy other files
def copy_additional_files(source_dir, dest_dir):
    files_to_copy = ["project.json", "workspaces.json", "job_manifest.json"]
    for filename in files_to_copy:
        source_path = os.path.join(source_dir, filename)
        dest_path = os.path.join(dest_dir, filename)
        if os.path.exists(source_path):
            try:
                with open(source_path, 'r') as f:
                    data = json.load(f)
                # If it's `project.json`, set "detached" to True
                if filename == "project.json":
                    data['detached'] = True
                with open(dest_path, 'w') as f:
                    json.dump(data, f, indent=4)
            except Exception as e:
                print(f"Error copying {filename}: {e}")

# Delete cs.lock file if it exists
def delete_cs_lock(dest_dir):
    cs_lock_path = os.path.join(dest_dir, "cs.lock")
    if os.path.exists(cs_lock_path):
        os.remove(cs_lock_path)


def main(source_dir, dest_dir, timeout, retries, logger, verbose, resume):
    try:
        base_dest_dir = "/root/cryosparc/common_workspace"
        if dest_dir:
            dest_dir = os.path.join(base_dest_dir, dest_dir)
        else:
            print("Error: DEST_DIR not provided.")
            sys.exit(1)

        project_name = os.path.basename(source_dir.rstrip('/'))
        dest_dir_project = os.path.join(dest_dir, project_name)

        # Check if we need to resume or start fresh
        if resume:
            logger.info("Resuming from previous synchronization...")
            files_to_sync, source_files, dest_files = compare_and_get_files_to_sync(source_dir, dest_dir_project, logger)
        else:
            logger.info(f"Checking for existing files in {source_dir}...")
            existing_files = []
            # Replacing os.walk with os.scandir
            for entry in os.scandir(source_dir):
                if entry.is_file():
                    existing_files.append(os.path.relpath(entry.path, source_dir))
                elif entry.is_dir():
                    # Handle subdirectories recursively
                    for subentry in os.scandir(entry.path):
                        if subentry.is_file():
                            existing_files.append(os.path.relpath(subentry.path, source_dir))

            files_to_sync = existing_files
            source_files = set(existing_files)
            dest_files = set()

        if files_to_sync:
            logger.info(f"Starting synchronization of {len(files_to_sync)} files to {dest_dir_project}.")
            sync_files(source_dir, dest_dir_project, files_to_sync, logger, verbose, tracked_files=set(files_to_sync), retries=retries)

        last_sync_time = time.time()
        tracked_files = set(files_to_sync)
        while True:
            new_files = monitor_directory(source_dir, tracked_files, timeout, retries, logger, source_files, dest_files)
            if new_files:
                logger.info(f"Found {len(new_files)} new files. Starting synchronization...")
                sync_files(source_dir, dest_dir_project, new_files, logger, verbose, tracked_files=tracked_files, retries=retries)
                last_sync_time = time.time()
            else:
                logger.info("No new files found after retries. Exiting.")
                break

        copy_additional_files(source_dir, dest_dir_project)
        delete_cs_lock(dest_dir_project)
    except KeyboardInterrupt:
        logger.warning("Script interrupted. Exiting...")
    finally:
        logger.info("File synchronization script completed.")

# Running the main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync files with logging.")
    parser.add_argument("source_dir", nargs="?", help="Source directory to monitor and sync.")
    parser.add_argument("dest_dir", nargs="?", help="Destination directory to sync files to.")
    parser.add_argument("--timeout", type=int, default=60, help="Time in seconds to wait between checks for new files.")
    parser.add_argument("--retries", type=int, default=5, help="Number of retries for checking new files.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output to console.")
    parser.add_argument("--resume", action="store_true", help="Resume the synchronization from the last successful state.")
    args = parser.parse_args()

    source_dir = os.environ.get("SOURCE_DIR") or args.source_dir
    dest_dir = os.environ.get("DEST_DIR") or args.dest_dir
    timeout = int(os.environ.get("TIMEOUT", args.timeout))
    retries = int(os.environ.get("RETRIES", args.retries))
    verbose = int(os.environ.get("VERBOSE", 0)) or int(args.verbose)
    resume = int(os.environ.get("RESUME", 0)) or int(args.resume)

    if not source_dir or not dest_dir:
        print("Error: Either set SOURCE_DIR and DEST_DIR as environment variables or pass them as arguments.")
        sys.exit(1)

    timeout = min(timeout, 600)  # Cap timeout at 600 seconds
    
    logger, error_log = setup_logging(dest_dir, os.path.basename(source_dir), verbose)
    # Log the configuration details
    logger.info("--- Configuration Context ---")
    logger.info(f"SOURCE_DIR: {source_dir}")
    logger.info(f"DEST_DIR: {dest_dir}")
    logger.info(f"TIMEOUT: {timeout} seconds")
    logger.info(f"RETRIES: {retries}")
    logger.info(f"VERBOSE: {'Enabled' if verbose else 'Disabled'}")
    logger.info(f"RESUME: {'Enabled' if resume else 'Disabled'}")
    logger.info("----------------------------")
    logger.info("Starting file synchronization script...")
    main(source_dir, dest_dir, timeout, retries, logger, verbose, resume)
