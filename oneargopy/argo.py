# -*- coding: utf-8 -*-
#initialize.py
#------------------------------------------------------------------------------
# Created By: Savannah Stephenson
# Creation Date: 05/30/2024
# Version: 1.0
#------------------------------------------------------------------------------
""" The argo class contains the primary functions for downloading and handling
    data gathered from GDAC.
"""
#------------------------------------------------------------------------------
# 
#
# Imports

# Local
from Settings import DownloadSettings, SourceSettings

# System
from pathlib import Path
import requests
from datetime import datetime
import shutil

class argo():
    """ The argo class which contains functions for downloading and handling 
        argo float data. 
    """

    def __init__(self): 
        self.download_settings = DownloadSettings()
        self.source_settings = SourceSettings()
        self.index_directory = Path(self.download_settings.base_dir.joinpath("Index"))


    def initialize(self) -> None:
        """ The initialize function downloads the index files form GDAC and 
            stores them in the proper directories defined in the 
            DownloadSettings class. 
        """
        print(f'Starting initialize process...')
        # Check for and create subdirectories if needed
        self.initialize_subdirectories()

        # Download files from GDAC to Index directory
        for file in self.download_settings.index_files:
            self.download_index_files(file)
        
        # Fill in avail_vars variable in the SourceSettings class
        self.source_settings.set_avail_vars()

        # Fill in dacs variable in the SourceSettings class
        self.source_settings.set_dacs()

        # Extract Unique floats from both data frames
            # There is some post processing that they do on unique floats in the initalize_argo.m
            # his any of that still relevant? 

        print(f'Initialize is finished!')

    def initialize_subdirectories(self) -> None:
        """ A function that checks for and creates the necessary folders as 
            listed in the download settings sub_dir list. 
        """
        print(f'Your current download settings are: {self.download_settings}')
        print(f'Checking for and creating necessary subdirectories...')
        for directory in self.download_settings.sub_dirs:
            directory_path = self.download_settings.base_dir.joinpath(directory)
            if directory_path.exists():
                print(f'The {directory_path} directory already exists!')
            else:
                try:
                    print(f'Creating the {directory} directory!')
                    directory_path.mkdir()
                except Exception as e:
                    print(f'Failed to create the {directory} directory: {e}')

    
    def download_index_files(self, file_name: str) -> None:
        """ A function to download and save an index file from GDAC sources. 

            :param: filename : str - The name of the file we are downloading.
        """
        print(f'Checking for and downloading index files...')
        # Get the expected filepath for the file
        file_path = self.index_directory.joinpath(file_name)

        # Check if the filepath exists
        if file_path.exists():

            # Check if the settings allow  for updates
            if self.download_settings.update == 0:
                print(f'The download settings have update set to 0, indicating that we do not want to update index files.')
            else: 
                last_modified_time = Path(file_path).stat().st_mtime
                print(f'The last modified time: {last_modified_time}')
                current_time = datetime.now().timestamp()
                print(f'The current time: {current_time}')
                seconds_since_modified = current_time - last_modified_time
                print(f'The seconds since modified: {seconds_since_modified}')
                print(f'The download threshold: {self.download_settings.update}')

                # Check if the file should be updated
                if (seconds_since_modified > self.download_settings.update):
                    print(f'The file: {file_name} needs to be updated.')
                    self.try_download(file_name ,True)
                else:
                    print(f'The file: {file_name} does not need to be updated yet.')

        # if the file doesn't exist then download it
        else: 
            print(f'The file: {file_name} needs to be downloaded.')
            self.try_download(file_name, False)

 
    def try_download(self, file_name: str, update_status: bool)-> None:
        """ A function that attempts to download a file from both GDAC sources.

            :param: file_name : str - The name of the file to download
            :param: update_status: bool - True if the file exists and we 
                are trying to update it. False if the file hasn't been 
                downloaded yet. 
        """
        success = False
        iterations = 0
        save_path = self.index_directory.joinpath(file_name)
        while (not success) and (iterations < self.download_settings.max_attempts):
            # Try both hosts (perfered one is listed first in download settings)
            for host in self.source_settings.hosts:
                url = "".join([host, file_name])
                print(f'URL we are trying to download from: {url}')
                print(f'WE are saving {file_name} to {save_path}')
                try:
                    with requests.get(url, stream=True) as r:
                        r.raise_for_status()
                        with open(save_path, 'wb') as f:
                            r.raw.decode_content = True
                            shutil.copyfileobj(r.raw, f)
                    success = True
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
        