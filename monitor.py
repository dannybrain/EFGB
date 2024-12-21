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

# Function to compare source and destination files and return files to be transferred
def compare_and_get_files_to_sync(source_dir, dest_dir, logger):
    source_files = set()
    dest_files = set()

    # Collect source files
    source_files_processed = 0  # Counter for processed source files
    logger.info(f"Scanning source directory: {source_dir} for files...")
    for root, _, files in os.walk(source_dir):
        for file in files:
            source_files.add(os.path.relpath(os.path.join(root, file), source_dir))
            source_files_processed += 1
            
            # Periodic update for every 4,000 files processed in the source directory
            if source_files_processed % 4000 == 0:
                logger.info(f"Still processing...")
         
    logger.info(f"Done ! Processed {source_files_processed} files in source directory.")

    # Collect destination files
    dest_files_processed = 0  # Counter for processed destination files
    logger.info(f"Scanning destination directory: {dest_dir} for files...")
    for root, _, files in os.walk(dest_dir):
        for file in files:
            dest_files.add(os.path.relpath(os.path.join(root, file), dest_dir))
            logger.debug("file at destination :" + os.path.relpath(os.path.join(root, file), dest_dir))
            dest_files_processed += 1

            # Periodic update for every 5000 files processed in the destination directory
            if dest_files_processed % 5000 == 0:
                logger.info(f"Still processing...")
    
    logger.info(f"Done ! Processed {dest_files_processed} files in destination directory.")

    # Files to be copied (present in source but not in destination)
    files_to_sync = list(source_files - dest_files)
    logger.info(f"Found {len(files_to_sync)} new or missing files that need to be transferred.")
    
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


# Monitor directory for new files
def monitor_directory(source_dir, tracked_files, timeout, retries, logger, source_files, dest_files):
    if logger:
        logger.info(f"Monitoring directory: {source_dir} for new files...")
    new_files = []
    files_processed = 0  # Counter for how many files we've processed

    for attempt in range(1, retries + 1):
        time.sleep(timeout)
        current_files = set()
        try:
            # Iterate through source directory and add new files to current_files if they meet the conditions
            for root, _, files in os.walk(source_dir):
                for file in files:
                    file_path = os.path.relpath(os.path.join(root, file), source_dir)
                    if file_path not in tracked_files and file_path not in dest_files:
                        current_files.add(file_path)
                    
                    # Periodic update for every 10,000 files processed
                    files_processed += 1
                    if files_processed % 4000 == 0:
                        logger.info(f"Still processing...")

            # If new files found, update tracked_files and break out
            if current_files:
                new_files = list(current_files)
                tracked_files.update(new_files)  # Add new files to tracked_files
                logger.info(f"Done ! Found {len(new_files)} new files.")
                break
            else:
                logger.info(f"No new files found. Retrying [{attempt}/{retries}]...")
        except Exception as e:
            logger.error(f"Error while monitoring directory: {e}")
    
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

# Main function
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
            for root, _, files in os.walk(source_dir):
                for file in files:
                    existing_files.append(os.path.relpath(os.path.join(root, file), source_dir))
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
