#!/usr/bin/env python3
import os
import time
import rpyc
from rpyc.utils.server import ThreadedServer
import logging

class FileScannerService(rpyc.Service):
    def __init__(self):
        self.last_scan_time = 0
        self.logger = self._setup_logging()

    def _setup_logging(self):
        logger = logging.getLogger("scanner_service")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
        return logger

    def exposed_scan_directory(self, directory, last_scan_time=None):
        """
        Scan directory for new or modified files since last_scan_time.
        Returns: list of file paths relative to directory
        """
        try:
            new_files = set()
            scan_time = time.time()
            
            self.logger.info(f"Scanning directory: {directory}")
            
            def scan_directory(current_dir, relative_path=''):
                with os.scandir(current_dir) as entries:
                    for entry in entries:
                        entry_relative_path = os.path.join(relative_path, entry.name)
                        try:
                            if entry.is_file():
                                if last_scan_time is None or entry.stat().st_mtime > last_scan_time:
                                    new_files.add(entry_relative_path)
                            elif entry.is_dir():
                                scan_directory(entry.path, entry_relative_path)
                        except OSError as e:
                            self.logger.error(f"Error accessing {entry.path}: {e}")
                            continue
            
            scan_directory(directory)
            self.logger.info(f"Scan complete. Found {len(new_files)} new/modified files")
            return list(new_files), scan_time
            
        except Exception as e:
            self.logger.error(f"Error scanning directory {directory}: {e}")
            raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="File Scanner RPC Service")
    parser.add_argument("--port", type=int, default=18861, help="Port to listen on")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    args = parser.parse_args()
    
    server = ThreadedServer(
        FileScannerService,
        hostname=args.host,
        port=args.port,
        protocol_config={
            'allow_public_attrs': True,
            'allow_all_attrs': True
        }
    )
    print(f"Starting RPC server on {args.host}:{args.port}")
    server.start()
