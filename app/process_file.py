# process_file.py
from concurrent.futures import ProcessPoolExecutor
import os
from genome_indexer import process_file_chunk, get_header_info, create_indices
from dotenv import load_dotenv
import signal
import sys
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('genome_processing.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()

NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", os.cpu_count()))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 10000))

def handle_interrupt(signal, frame):
    logging.info("\nProcess cancelled by user.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

def process_file_parallel(file_path: str):
    start_time = time.time()
    start_datetime = datetime.now()
    
    logging.info(f"Starting processing of {file_path}")
    logging.info(f"Start time: {start_datetime}")
    
    # Get file size
    file_size = os.path.getsize(file_path)
    logging.info(f"File size: {file_size / (1024*1024):.2f} MB")
    
    # Get header information
    sample_columns, column_positions = get_header_info(file_path)
    logging.info(f"Found {len(sample_columns)} sample columns")
    
    # Create indices first
    create_indices()
    
    chunk_positions = []
    total_lines = 0
    with open(file_path, 'r') as f:
        while True:
            pos = f.tell()
            # Read CHUNK_SIZE lines
            lines = [f.readline() for _ in range(CHUNK_SIZE)]
            if not lines[-1]:  # EOF
                break
            total_lines += len(lines)
            chunk_positions.append((pos, len(lines)))
    
    logging.info(f"Total lines to process: {total_lines}")
    logging.info(f"Number of chunks: {len(chunk_positions)}")

    # Process chunks in parallel
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        futures = []
        for start_pos, chunk_len in chunk_positions:
            futures.append(
                executor.submit(
                    process_file_chunk,
                    file_path,
                    start_pos,
                    chunk_len,
                    column_positions
                )
            )
        
        # Monitor progress
        completed = 0
        for future in futures:
            future.result()
            completed += 1
            if completed % 10 == 0:
                progress = (completed / len(chunk_positions)) * 100
                elapsed = time.time() - start_time
                estimated_total = elapsed / (completed / len(chunk_positions))
                remaining = estimated_total - elapsed
                
                logging.info(
                    f"Progress: {progress:.1f}% - "
                    f"Elapsed: {elapsed/60:.1f} minutes - "
                    f"Estimated remaining: {remaining/60:.1f} minutes"
                )
    
    end_time = time.time()
    end_datetime = datetime.now()
    total_time = end_time - start_time
    
    logging.info(f"Processing completed at: {end_datetime}")
    logging.info(f"Total processing time: {total_time/60:.2f} minutes")
    logging.info(f"Average processing speed: {total_lines/total_time:.0f} lines/second")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_file.py <vcf_file_path>")
        sys.exit(1)
    
    process_file_parallel(sys.argv[1])