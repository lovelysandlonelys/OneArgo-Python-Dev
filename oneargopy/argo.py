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

class argo():
    """ The argo class which contains functions for downloading and handling argo float data. 
    """

    def __init__(self): 
        self.download_settings = DownloadSettings()
        self.source_settings = SourceSettings()


    def initialize(self) -> None:
        """ The initialize function downloads the index files form GDAC and stores them in
            the proper directories defined in the DewnloadSettings class. 
        """
        # Check for and create subdirectories if needed
        print(f'Your current download settings are: {self.download_settings}')
        print(f'Checking for and creating necessary subdirectories...')
        for directory in self.download_settings.sub_dirs:
            directory_path = self.download_settings.base_dir.joinpath(directory)
            if directory_path.exists():
                print(f'The {directory_path} directory already exists!')
            else:
                print(f'Creating the {directory} directory!')
                directory_path.mkdir()

        # Download files from GDAC to Index directory
        index_directory = self.download_settings.base_dir.joinpath("Index")
        if index_directory: 
            for file in self.download_settings.index_files:
                self.download_file(file, index_directory, self.source_settings.hosts)
        else: 
            print(f'The Index directory does not exist, we need the index directory to save the index files to.')

        # Fill in avail_vars variable in the SourceSettings class
        # Fill in dacs variable in the SourceSettings class

        # Extract Unique floats from both data frames
            # There is some post processing that they do on unique floats in the initalize_argo.m
            # his any of that still relevant? 

    
    def download_file(self, file_name: str, save_point: Path) -> None:
        """ A function to download and save a file from GDAC sources. 

            :param: filename : str - The name of the file we are downloading.
            :param: savepoint : Path - The directory that we are saving the file to.
            :param: hosts : list - A list of URLs to the GDAC sources. 
        """
        # Check if the file we want to download is already at our savepoint
        file_path = save_point.joinpath(file_name)
        if file_path.exists():
            # Check if the file needs to be updated
            if (self.download_settings.update > 0): 
                last_modified_time = Path(file_path).stat().st_mtime
                current_time = datetime.now().timestamp()
                seconds_since_modified = current_time - last_modified_time
                if (seconds_since_modified > self.download_settings.update):
                    # Download and replace file
                    pass
                pass
                

        # If the file isn't there OR it needs to be updated then 
            # if connection failed try both sources number of times that the general settins says
                # First try primary host
                
                # Try secondary host
            # if the file never got downloaded then issue a warning if an old file exists locally
            # if it doesn't exist locally then raise an exception


    # Rewrite to be iterative rather than recursive because we don't want to be stuck
    # trying one of the hosts FOREVER when we have two sources to look at. 
    def try_download(self, url: str):
        """ A recursive function to return a request from a URL in the case of there
            being a connection error. Function taken from GO-BGC Python tutorial and 
            slightly altered.

            :param: url : str - The URL to download the file.
        """
        try:
            return requests.get(url,stream=True,auth=None,verify=False)
        except requests.exceptions.ConnectionError as error_tag:
            print('Error connecting:',error_tag)
            time.sleep(1)
            return self.try_download(url)
        

