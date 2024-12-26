#!/usr/bin/env python3
import os
import shutil
import time
import sys
import argparse
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict
import rpyc

# Function to compare source and destination files and return files to be transferred
def compare_and_get_files_to_sync(source_dir, dest_dir, logger, rpc_connection=None, last_scan_time=None):
    source_files = set()
    dest_files = set()

    def collect_files(dir_path, relative_to):
        files_set = set()
        try:
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    files_set.add(os.path.relpath(os.path.join(root, file), relative_to))
        except Exception as e:
            logger.error(f"Error traversing directory {dir_path}: {e}")
        return files_set

    # If RPC connection is available, use it for source directory
    if rpc_connection:
        try:
            source_files_list, scan_time = rpc_connection.root.scan_directory(source_dir, last_scan_time)
            source_files = set(source_files_list)
            logger.info(f"Retrieved {len(source_files)} files from RPC scanner")
        except Exception as e:
            logger.error(f"RPC scan failed, falling back to local scan: {e}")
            rpc_connection = None

    # If no RPC or RPC failed, use original local scanning method
    if not rpc_connection:
        logger.info(f"Scanning source directory: {source_dir}...")
        source_files = collect_files(source_dir, source_dir)
        logger.info(f"Completed scanning source directory. Found {len(source_files)} files.")

    logger.info(f"Scanning destination directory: {dest_dir}...")
    dest_files = collect_files(dest_dir, dest_dir)
    logger.info(f"Completed scanning destination directory. Found {len(dest_files)} files.")

    files_to_sync = list(source_files - dest_files)
    logger.info(f"Identified {len(files_to_sync)} files to sync.")

    return files_to_sync, source_files, dest_files

# Function to setup logging
def setup_logging(dest_dir, project_name, verbose):
    os.makedirs(dest_dir, exist_ok=True)
    dest_dir_project = os.path.join(dest_dir, project_name)
    os.makedirs(dest_dir_project, exist_ok=True)

    logger = logging.getLogger("sync_logger")
    logger.setLevel(logging.DEBUG)

    # Error log
    error_log = os.path.join(dest_dir_project, "error_log.txt")
    error_handler = logging.FileHandler(error_log)
    error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    error_handler.setLevel(logging.ERROR)
    logger.addHandler(error_handler)

    # Console log
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"))
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    if verbose:
        sync_log = os.path.join(dest_dir_project, "sync_log.txt")    
        try:
            file_handler = logging.FileHandler(sync_log)
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to initialize sync_log.txt: {e}")

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
def sync_files(source_dir, dest_dir, files_to_sync, logger, verbose, batch_size=100, retries=3):
    os.makedirs(dest_dir, exist_ok=True)
    total_files = len(files_to_sync)
    synced_files_count = 0
    synced_files_lock = Lock()

    logger.info(f"Starting synchronization of {total_files} files to {dest_dir}.")
    file_batches = [files_to_sync[i:i + batch_size] for i in range(0, total_files, batch_size)]

    def sync_batch(batch):
        try:
            results = copy_file_batch(batch, source_dir, dest_dir, logger, verbose, retries)
            with synced_files_lock:
                nonlocal synced_files_count
                synced_files_count += len(results)
            return results
        except Exception as e:
            logger.error(f"Error syncing batch: {e}")
            return []

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(sync_batch, batch) for batch in file_batches]
        for future in as_completed(futures):
            future.result()
            with synced_files_lock:
                current_progress = int((synced_files_count / total_files) * 100)
                logger.info(f"Progress: {current_progress}% , files [{synced_files_count}/{total_files}]")

    logger.info(f"Total files synced: {total_files}.")

# Function to get scanner connection
def get_scanner_connection(host, port, logger):
    try:
        return rpyc.connect(host, port)
    except Exception as e:
        logger.error(f"Failed to connect to scanner service: {e}")
        return None

# Main function
def main(source_dir, dest_dir, timeout, retries, logger, verbose, resume, scanner_host=None, scanner_port=None):
    try:
        dest_dir_project = os.path.join(dest_dir, os.path.basename(source_dir.rstrip('/')))
        os.makedirs(dest_dir_project, exist_ok=True)

        # Initialize RPC connection if host is provided
        rpc_connection = None
        if scanner_host:
            rpc_connection = get_scanner_connection(scanner_host, scanner_port, logger)
            if rpc_connection:
                logger.info(f"Connected to scanner service at {scanner_host}:{scanner_port}")

        last_scan_time = 0 if not resume else time.time() - timeout

        while True:
            # If RPC connection failed, try to reconnect
            if scanner_host and not rpc_connection:
                rpc_connection = get_scanner_connection(scanner_host, scanner_port, logger)

            files_to_sync, source_files, dest_files = compare_and_get_files_to_sync(
                source_dir, dest_dir_project, logger, rpc_connection, last_scan_time
            )

            if files_to_sync:
                sync_files(source_dir, dest_dir_project, files_to_sync, logger, verbose, retries=retries)
            else:
                logger.info("No new files to sync.")

            if rpc_connection:
                last_scan_time = time.time()

            logger.info(f"Waiting {timeout} seconds before next check...")
            time.sleep(timeout)

    except KeyboardInterrupt:
        logger.warning("Script interrupted. Exiting...")
    finally:
        if rpc_connection:
            rpc_connection.close()
        logger.info("File synchronization script completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync files with optimized NFS traversal.")
    parser.add_argument("source_dir", help="Source directory to monitor and sync.")
    parser.add_argument("dest_dir", help="Destination directory to sync files to.")
    parser.add_argument("--timeout", type=int, default=60, help="Time in seconds to wait between checks for new files.")
    parser.add_argument("--retries", type=int, default=5, help="Number of retries for checking new files.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output to console.")
    parser.add_argument("--resume", action="store_true", help="Resume the synchronization from the last successful state.")
    # New optional arguments for RPC
    parser.add_argument("--scanner-host", help="Host running the scanner service (optional)")
    parser.add_argument("--scanner-port", type=int, default=18861, help="Port of the scanner service")
    args = parser.parse_args()

    source_dir = args.source_dir
    dest_dir = args.dest_dir
    timeout = args.timeout
    retries = args.retries
    verbose = args.verbose
    resume = args.resume
    scanner_host = args.scanner_host
    scanner_port = args.scanner_port


    logger, error_log = setup_logging(dest_dir, os.path.basename(source_dir), verbose)

    logger.info("Starting file synchronization script...")
    main(source_dir, dest_dir, timeout, retries, logger, verbose, resume, scanner_host, scanner_port)
