#!/usr/bin/env python3
"""
######################################################################
# CryoSparc Project File Synchronization Script
#
# This script is used to synchronize CryoSparc project files from the
# Local CryoSparc instance to Magellan servers. It is designed to be
# called from a Magellan pipeline.
# The script checks for new files every X seconds and syncs them
# to the destination server.
#
# The script performs the following:
# - Initial synchronization of files from the source directory to the destination.
# - Ongoing monitoring of the source directory for new files and synchronization.
# - Logs all synchronized files in the log file (sync_log.txt).
#
# Arguments:
# - source_dir: The source directory for the Cryosparc project files.
# - dest_dir: The destination directory for the files.
# - --timeout: The time in seconds to wait between checks for new files (default: 5 seconds).
# - --no-log: Disable log file creation and sync file logging.
# - --no-progress: Disable the progress bar display during sync.
#
# Environment Variables:
# - SOURCE_DIR: Override source directory.
# - DEST_DIR: Override destination directory.
# - SYNC_LOG: Override sync log file name.
# - VERBOSE: Enable verbose output (set to "true" or "false").
# - NO_PROGRESS: Disable progress bar (set to "true" or "false").
# - NO_LOG: Skip sync log (set to "true" or "false").
#
######################################################################
"""
import os
import subprocess
import time
from tqdm import tqdm
import sys
import argparse

SYNC_LOG = "sync_log.txt"
ERROR_LOG = "error_log.txt"

# Function to write logs to a file
def write_log(log_file, message):
    with open(log_file, 'a') as log:
        log.write(message + "\n")

# Syncing function with progress bar update for each subdirectory
def sync_files(source_dir, dest_dir, files_to_sync, log_sync, show_progress):
    os.makedirs(dest_dir, exist_ok=True)

    # Group files by subdirectory (e.g., 'J1', 'J2', etc.)
    subdirs = {}
    for file in files_to_sync:
        subdir = os.path.dirname(file)
        if subdir not in subdirs:
            subdirs[subdir] = []
        subdirs[subdir].append(file)

    # Iterate over each subdirectory and sync its files
    total_files = len(files_to_sync)
    total_subdirs = len(subdirs)
    for subdir, files in subdirs.items():
        subdir_source = os.path.join(source_dir, subdir)
        subdir_dest = os.path.join(dest_dir, subdir)

        # Create a progress bar for the subdirectory if required
        progress_bar = None
        if show_progress:
            progress_bar = tqdm(total=len(files), desc=f"Syncing {subdir}", unit="file")

        # Rsync command with `-q` for quiet mode (only error messages)
        rsync_cmd = [
            "rsync",
            "-aq",  # Archive mode, quiet mode
            subdir_source + '/',  # Source subdirectory
            subdir_dest + '/'     # Destination subdirectory
        ]

        try:
            print(f"Syncing subdirectory {subdir} from {subdir_source} to {subdir_dest}...")
            process = subprocess.Popen(rsync_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()

            # Log rsync output for each file (stderr for errors)
            if stdout and log_sync:
                write_log(SYNC_LOG, stdout.decode())
            if stderr and log_sync:
                write_log(ERROR_LOG, stderr.decode())

            # After rsync completes, manually log files that were synced (only file names with full path)
            if log_sync:
                for file in files:
                    # Log the full file path for each file
                    full_file_path = os.path.join(source_dir, file)
                    write_log(SYNC_LOG, full_file_path)
                    if progress_bar:
                        progress_bar.update(1)

            # Close progress bar if we have one
            if progress_bar:
                progress_bar.close()

        except Exception as e:
            write_log(ERROR_LOG, f"Error during rsync for {subdir_source}: {e}")
            print(f"Error syncing {subdir_source}: {e}")

# Directory monitoring function using timestamp mechanism
def monitor_directory(source_dir, last_sync_time, timeout):
    print(f"Monitoring directory: {source_dir} for new files...")

    # Sleep before checking again
    time.sleep(timeout)

    # List all files in the directory and its subdirectories
    new_files = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            file_path = os.path.join(root, file)
            # Get the timestamp of the last modification of the file
            file_mod_time = os.path.getmtime(file_path)

            # Check if the file was modified or created after the last sync time
            if file_mod_time > last_sync_time:
                new_files.append(file_path)

    if new_files:
        print(f"Found {len(new_files)} new or modified files.")
    else:
        print("No new or modified files found.")

    return new_files

# Main function with graceful exit on CTRL+C
def main(source_dir, dest_dir, timeout, log_sync, show_progress):
    # Ensure log files are deleted if they exist
    if log_sync:
        if os.path.exists(SYNC_LOG):
            os.remove(SYNC_LOG)
        if os.path.exists(ERROR_LOG):
            os.remove(ERROR_LOG)

    # Ensure log files are created at the start if logging is enabled
    if log_sync:
        open(SYNC_LOG, 'w').close()
        open(ERROR_LOG, 'w').close()

    try:
        # Perform initial sync of all existing files
        print(f"Checking for existing files in {source_dir}...")
        existing_files = []
        # Walk through the directory to find files inside subdirectories (e.g., J1, J2, ...)
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                existing_files.append(os.path.relpath(os.path.join(root, file), source_dir))

        if existing_files:
            print(f"Found {len(existing_files)} existing files. Starting initial sync...")
            sync_files(source_dir, dest_dir, existing_files, log_sync, show_progress)
        else:
            print(f"No existing files found in {source_dir}. Waiting for new files to appear...")

        # Get the timestamp of the last sync (current time)
        last_sync_time = time.time()

        # Monitor for new files after the initial sync
        while True:
            new_files = monitor_directory(source_dir, last_sync_time, timeout=timeout)

            if new_files:
                print(f"Found {len(new_files)} new files. Starting synchronization...")
                sync_files(source_dir, dest_dir, new_files, log_sync, show_progress)
                # Update the last sync timestamp to the current time after a successful sync
                last_sync_time = time.time()
            else:
                print("No new files found. Exiting.")
                break

    except KeyboardInterrupt:
        print("\nScript interrupted. Saving logs and exiting...")
        # Ensure logs are saved before exiting
        sys.exit(0)  # Exit gracefully after logging

# Running the main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync files with progress and logging.")
    parser.add_argument("source_dir", help="Source directory to monitor and sync.")
    parser.add_argument("dest_dir", help="Destination directory to sync files to.")
    parser.add_argument("--timeout", type=int, default=5, help="Time in seconds to wait between checks for new files.")
    parser.add_argument("--no-log", action="store_true", help="Disable log file creation and sync file logging.")
    parser.add_argument("--no-progress", action="store_true", help="Disable the progress bar display.")
    args = parser.parse_args()

    # Read from environment variables if set
    source_dir = os.environ.get("SOURCE_DIR", args.source_dir)
    dest_dir = os.environ.get("DEST_DIR", args.dest_dir)
    timeout = int(os.environ.get("TIMEOUT", args.timeout))
    log_sync = not os.environ.get("NO_LOG", args.no_log)
    show_progress = not os.environ.get("NO_PROGRESS", args.no_progress)

    print(f"Starting file synchronization script...")
    main(source_dir, dest_dir, timeout, log_sync, show_progress)

