# -*- coding: utf-8 -*-
#setup_functions.py
#------------------------------------------------------------------------------
# Created By: Savannah Stephenson
# Creation Date: 06/05/2024
# Version: 1.0
#------------------------------------------------------------------------------
""" This module contains functions used in argo.py to setup directories and 
    download files.
"""
#------------------------------------------------------------------------------
# 
#
# Imports

# Local
from Settings import DownloadSettings, SourceSettings, AnalysisSettings

# System
import requests
from pathlib import Path
from datetime import datetime
import shutil
import gzip
import json

def initialize_subdirectories(download_settings: DownloadSettings) -> None:
    """ A function that checks for and creates the necessary folders as 
        listed in the download settings sub_dir list. 

        :param: download_settings: DownloadSettings - An instance of the
            DownloadSettings class containing the current download
            settings when the function is called by argo.py.
    """
    for directory in download_settings.sub_dirs:
        directory_path = download_settings.base_dir.joinpath(directory)
        if directory_path.exists():
            print(f'The {directory_path} directory already exists!')
        else:
            try:
                print(f'Creating the {directory} directory!')
                directory_path.mkdir()
            except Exception as e:
                print(f'Failed to create the {directory} directory: {e}')


def download_index_files(file_name: str, download_settings: DownloadSettings, source_settings: SourceSettings) -> None:
    """ A function to download and save an index file from GDAC sources. 

        :param: filename : str - The name of the file we are downloading.
        :param: download_settings : DownloadSettings - An instance of the
            DownloadSettings class containing the current download
            settings when the function is called by argo.py.
        :param: source_settings : SourceSettings - An instance of the
            SourceSettings class containing the current Source
            settings when the function is called by argo.py.
    """
    index_directory = Path(download_settings.base_dir.joinpath("Index"))

    # Get the expected filepath for the file
    file_path = index_directory.joinpath(file_name)
    gz_file_path = index_directory.joinpath("".join([file_name, ".gz"]))

    # Check if the filepath exists
    if file_path.exists():

        # Check if the settings allow  for updates
        if download_settings.update == 0:
            print(f'The download settings have update set to 0, indicating that we do not want to update index files.')
        else: 
            txt_last_modified_time = Path(file_path).stat().st_mtime
            print(f'The last modified time .txt: {txt_last_modified_time}')
            gz_last_modified_time = Path(gz_file_path).stat().st_mtime
            print(f'The last modified time .gx: {gz_last_modified_time}')
            
            current_time = datetime.now().timestamp()
            print(f'The current time: {current_time}')
            txt_seconds_since_modified = current_time - txt_last_modified_time
            gz_seconds_since_modified = current_time - gz_last_modified_time
            print(f'The seconds since modified txt: {txt_seconds_since_modified}')
            print(f'The seconds since modified gz: {gz_seconds_since_modified}')
            print(f'The download threshold: {download_settings.update}')

            # Check if the file should be updated
            if (gz_seconds_since_modified > download_settings.update):
                print(f'The file: {file_name} needs to be updated.')
                try_download(file_name ,True, download_settings, source_settings)
            else:
                print(f'The file: {file_name} does not need to be updated yet.\n')

    # if the file doesn't exist then download it
    else: 
        print(f'The file: {file_name} needs to be downloaded.')
        try_download(file_name, False, download_settings, source_settings)


def try_download(file_name: str, update_status: bool, download_settings: DownloadSettings, source_settings: SourceSettings)-> None:
    """ A function that attempts to download a file from both GDAC sources.

        :param: file_name : str - The name of the file to download
        :param: update_status: bool - True if the file exists and we 
            are trying to update it. False if the file hasn't been 
            downloaded yet. 
        :param: download_settings : DownloadSettings - An instance of the
            DownloadSettings class containing the current download
            settings when the function is called by argo.py.
        :param: source_settings : SourceSettings - An instance of the
            SourceSettings class containing the current Source
            settings when the function is called by argo.py.
    """
    index_directory = Path(download_settings.base_dir.joinpath("Index"))

    success = False
    iterations = 0
    txt_save_path = index_directory.joinpath(file_name)
    gz_save_path = index_directory.joinpath("".join([file_name, ".gz"]))

    while (not success) and (iterations < download_settings.max_attempts):
        # Try both hosts (perfered one is listed first in download settings)
        for host in source_settings.hosts:

            url = "".join([host, file_name, ".gz"])

            print(f'URL we are trying to download from: {url}')
            print(f'WE are saving {file_name}.gz to {gz_save_path}')

            try:
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    with open(gz_save_path, 'wb') as f:
                        r.raw.decode_content = True
                        shutil.copyfileobj(r.raw, f)

                print(f'Unzipping {file_name}.gz...')
                with gzip.open(gz_save_path, 'rb') as gz_file:
                    with open(txt_save_path, 'wb') as txt_file:
                        shutil.copyfileobj(gz_file, txt_file)
                
                success = True
                print(f'{file_name}.gz was successfully downloaded and unzipped to {file_name}')
                
                # Exit the loop if download is successful so we don't try additional
                # sources for no reason.
                break 

            except requests.RequestException as e:
                print(f'Error encountered: {e}. Trying next host...')
        
        # Increment Iterations
        iterations += 1

    # If ultimately nothing could be downloaded
    if not success: 
        message = 'Update failed:' if update_status else 'Download failed:'
        raise Exception(f'{message} {file_name} could not be downloaded at this time.')


def parse_settings(user_settings: str) -> tuple[str, str]:
    """ A function to parse a given user_settings file to initialize
        the Settings classes based off of a passed path to a json
        file. 

        :param: user_settings : str - A string that will be converted into
            a path to parse a json from.

        :returns: download_settings, source_settings : tuple[str, str] - We
            return a tuple holding download settings and source settings.
        
        NOTE: In the function we also parse analysis settings, but we do not
            pass these settings back to argo because at current implementation
            we do not utilize the analysis settings inside of argo.py. 
    """
    path = Path(user_settings)

    with path.open('r', encoding='utf-8') as file:
        data = json.load(file)

    # Parse DownloadSettings
    ds_data = data['DownloadSettings']
    download_settings = DownloadSettings(
        base_dir=Path(ds_data['base_dir']),
        sub_dirs=ds_data['sub_dirs'],
        index_files=ds_data['index_files'],
        verbose=ds_data['verbose'],
        update=ds_data['update'],
        max_attempts=ds_data['max_attempts']
    )
    
    # Parse AnalysisSettings
    as_data = data['AnalysisSettings']
    analysis_settings = AnalysisSettings(
        temp_thresh=as_data['temp_thresh'],
        dens_thresh=as_data['dens_thresh'],
        interp_lonlat=as_data['interp_lonlat']
    )
    
    # Parse SourceSettings
    ss_data = data['SourceSettings']
    source_settings = SourceSettings(
        hosts=ss_data['hosts'], 
        avail_vars=ss_data['avail_vars'],
        dacs=ss_data['dacs']
    )
        
    return download_settings, source_settings