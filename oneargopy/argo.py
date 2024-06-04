# -*- coding: utf-8 -*-
#initialize.py
#----------------------------------
# Created By: Savannah Stephenson
# Creation Date: 05/30/2024
# Version: 1.0
#----------------------------------
""" The argo class contains the primary functions for downloading and handling
    data gathered from GDAC.
"""
#----------------------------------
# 
#
# Imports

# Local
from Settings import DownloadSettings, SourceSettings

# System
from pathlib import Path
import requests
import time
import datetime
import shutil

class argo():
    """ The argo class which contains functions for downloading and handling argo float data. 
    """

    def __init__(self): 
        self.download_settings = DownloadSettings()
        self.source_settings = SourceSettings()
        self.index_directory = self.download_settings.base_dir.joinpath("Index")


    def initialize(self) -> None:
        """ The initialize function downloads the index files form GDAC and stores them in
            the proper directories defined in the DewnloadSettings class. 
        """
        # Check for and create subdirectories if needed
        self.initialize_subdirectories()

        # Download files from GDAC to Index directory
        if self.index_directory.exists(): 
            for file in self.download_settings.index_files:
                self.download_index_files(file)
        else: 
            print(f'The Index directory does not exist, we need the index directory to save the index files to.')

        # Fill in avail_vars variable in the SourceSettings class
        # Fill in dacs variable in the SourceSettings class

        # Extract Unique floats from both data frames
            # There is some post processing that they do on unique floats in the initalize_argo.m
            # his any of that still relevant? 

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
        # Get the expected filepath for the file
        file_path = self.index_directory.joinpath(file_name)

        # Check if the filepath exists
        if file_path.exists():

            # Check if the settings allow  for updates
            if (self.download_settings.update == 0):
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


    # Download profiles function
    def download_netdcf_files() -> None:
        pass

 
    def try_download(self, file_name: str, update_status: bool):
        """ A function that attempts to download a file from both GDAC sources.

            :param: url : str - The URL to download the file.
            :param: update_status: bool - True if the file exists and we are trying to update it. False if the file hasn't been downloaded yet. 
        """
        success = False
        iterations = 0
        while(not success and iterations < self.download_settings.try_download):
            # Try the first host

            # Try the second host
        # If ultimatly nothing could be downloaded
        if(not success): 
            pass
        

