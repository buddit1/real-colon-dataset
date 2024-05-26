#!/usr/bin/env python

""" Download the REAL-Colon dataset from figshare and extract all the files

    Usage:
        - Update download_dir = './dataset/' with path to the folder to download the REAL-colon dataset
        - python3 figshare_dataset.py

    Copyright 2023-, Cosmo Intelligent Medical Devices
"""
import os
import requests
import tarfile
import time
from multiprocessing import Pool
import tempfile
import logging

import dotenv
import tqdm
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient


# Define Figshare article URL and API endpoint
article_url = 'https://api.figshare.com/v2/articles/22202866'

# Specify the path to your custom CA bundle here, or set to None to use the default CA bundle
custom_ca_bundle = None

# Specify the path where to download the dataset
CONTAINER_NAME = "polyp-datasets"
BASE_BLOB_NAME = "real-colon-dataset"


formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('download_real_colon_dataset.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


# Helper function to return the file size if a path exists, -1 otherwise
def file_exists(local_filename):
    if os.path.exists(local_filename):
        return os.path.getsize(local_filename)
    return -1


def blob_exists(local_filename, container_client: ContainerClient, base_blob_name):
    name_wo_ext = local_filename[:local_filename.find('.')]
    blob_path = os.path.join(base_blob_name, name_wo_ext)
    blobs_list = container_client.list_blob_names(name_starts_with=blob_path)
    blob_exists = False
    for blob in blobs_list:
        blob_exists = True
        break
    return blob_exists


def download_file(args):
    url, local_filename = args
    simple_filename = local_filename
    existing_file_size = file_exists(local_filename)

    max_attempts = 1000  # Maximum number of download attempts
    attempt = 0
    retry_delay = 180  # Wait for 3 minutes (180 seconds) before retrying

    while attempt < max_attempts:
        try:
            # get request using the CA verification if provided
            with requests.get(url, stream=True, verify=custom_ca_bundle or True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                downloaded_size = 0
                start_time = time.time()

                # Download the file from figshare and record the size and time it took
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                    elapsed_time = time.time() - start_time
                    minutes, seconds = divmod(int(elapsed_time), 60)
                print()  # Print a newline after download completion
            return local_filename

        # If there is an error, wait for 180 seconds and try to continue where you left off
        except Exception as e:
            error_message = str(e)
            if 'IncompleteRead' in error_message:
                print(f'Connection error occurred: {error_message}. Retrying in {retry_delay} seconds...')
                time.sleep(retry_delay)  # Wait for the specified delay
                attempt += 1
            else:
                print(f'An unexpected error occurred: {error_message}. Retrying in {retry_delay} seconds...')
                time.sleep(retry_delay)  # Wait for the specified delay
                attempt += 1

    print(f'Failed to download {simple_filename} after {max_attempts} attempts.')
    return None


def extract_file(args):
    file_path, download_dir = args

    # If there is a compressed .tar.gz file and the unzipped directory doesn't already exist, then extract it
    if file_path.endswith('.tar.gz'):
        file_comp_path = os.path.join(download_dir, file_path)
        file_name = file_path.rstrip('.tar.gz')
        extracted_folder_name = os.path.splitext(os.path.basename(file_name))[0]
        extracted_folder_path = os.path.join(download_dir, extracted_folder_name)

        if not os.path.exists(extracted_folder_path):
            print(f'Extracting {file_path}...')
            with tarfile.open(file_comp_path, 'r') as tar_ref:
                tar_ref.extractall(download_dir)

        # Delete the tar.gz file ~
        print(f'Deleting {file_path}...')
        os.remove(file_comp_path)


def extract_files(file_paths, download_dir):
    # Control the number of processes for extraction
    num_processes = 3  # Change this to the desired number of extraction processes
    num_processes = min(num_processes, len(file_paths))

    # Create a pool of worker processes for extraction
    pool = Pool(processes=num_processes)

    # Map the extraction function to the file paths
    pool.map(extract_file, [(file_path, download_dir) for file_path in file_paths])


def download_to_blob(file_url, file_name, storage_acct_url: str,
                     container_name: str, base_blob_name: str):
    #have to initialize blob service stuff in here to play nice with multiprocessing
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(storage_acct_url, credential=credential)
    container_exists = False
    containers = blob_service_client.list_containers(name_starts_with=container_name)
    for container in containers:
        if container.name == container_name:
            container_exists = True
            break
    if not container_exists:
        container_client = blob_service_client.create_container(container_name)
    else:
        container_client = blob_service_client.get_container_client(container_name)
    n_blobs_uploaded = 0
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            start = time.time()
            local_file_path = os.path.join(tmpdir, file_name)
            download_file((file_url, local_file_path))
            downloaded_time = time.time() - start
            download_minutes = downloaded_time // 60
            download_seconds = downloaded_time % 60
            logger.info(f"downloading file {file_name} took {download_minutes} minutes and {download_seconds:.2f} seconds")
            extract_start = time.time()
            with tarfile.open(local_file_path, 'r') as tar_ref:
                    tar_ref.extractall(tmpdir)
            extract_time = time.time() - extract_start
            extract_minutes = extract_time // 60
            extract_seconds = extract_time % 60
            logger.info(f"extracting file {file_name} took {extract_minutes} minutes and {extract_seconds:.2f} seconds")
            os.remove(local_file_path)
            upload_start = time.time()
            for root, dirs, files in os.walk(tmpdir):
                for f in files:
                    full_path = os.path.join(root, f)
                    rel_path = os.path.relpath(full_path, tmpdir)
                    blob_path = os.path.join(base_blob_name, rel_path)
                    with open(full_path, 'rb') as data:
                        container_client.upload_blob(name=blob_path, data=data)
                        n_blobs_uploaded += 1
            upload_time = time.time() - upload_start
            upload_minutes = upload_time // 60
            upload_seconds = upload_time % 60
            logger.info(f"uploading blobs for {file_name} took {upload_minutes} minutes and {upload_seconds:.2f} seconds")
        total_time = time.time() - start
        total_minutes = total_time // 60
        total_seconds = total_time % 60
        logger.info(f"total time for {file_name} was {total_minutes} minutes and {total_seconds:.2f} seconds")
    except Exception as e:
        logger.exception(e)
        return -1
    return n_blobs_uploaded

def main():
    dotenv.load_dotenv()
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    default_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)

    container_exists = False
    containers = blob_service_client.list_containers(name_starts_with=CONTAINER_NAME)
    for container in containers:
        if container.name == CONTAINER_NAME:
            container_exists = True
            break
    if not container_exists:
        container_client = blob_service_client.create_container(CONTAINER_NAME)
    else:
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    response = requests.get(article_url, verify=custom_ca_bundle or True)
    response.raise_for_status()
    article_data = response.json()

    download_tasks = []
    for file_info in article_data['files']:

        # Get the file names
        file_url = file_info['download_url']
        file_name = file_info['name']
        existing_blob = blob_exists(file_name, container_client, BASE_BLOB_NAME)
        if existing_blob:
            logger.info(f"blobs for file {file_name} already uploaded. Skipping")
            continue
        download_tasks.append((file_url, file_name, account_url, CONTAINER_NAME, BASE_BLOB_NAME))
        logger.info(f'Queued {file_name} for download...')

    # Control the number of processes by adjusting this variable
    num_processes = 4  # Change this to the desired number of processes

    # Ensure the number of processes does not exceed the number of tasks
    num_processes = min(num_processes, len(download_tasks))

    # Create a pool of worker processes and download files concurrently
    if num_processes != 0:
        with Pool(processes=num_processes) as pool:
            results = pool.starmap(download_to_blob, tqdm.tqdm(download_tasks, total=len(download_tasks)))

    fnames = [args[1] for args in download_tasks]
    for file, n_blobs in zip(fnames, results):
        if n_blobs == -1:
            logger.error(f"Something went wrong for file {file}. See exception logs for stack trace.")
        else:
            logger.info(f"Uploaded {n_blobs} for file {file}")
    logger.info('All downloads completed.')


if __name__ == "__main__":
    main()
