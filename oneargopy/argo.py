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

class argo():
    """ The argo class which contains functions for downloading and handling argo float data. 
    """


    def initialize() -> None:
        """ The initialize function downloads the index files form GDAC and stores them in
            the proper directories defined in the DewnloadSettings class. 
        """
        # Instantiating DownloadSettings and SourceSettings
        # classes to use default settings of both.
        download_settings = DownloadSettings()
        source_settings = SourceSettings()

        # Check for and create subdirectories if needed
        print(f'Your current download settings are: {download_settings}')
        print(f'Checking for and creating necessary subdirectories...')
        for directory in download_settings.sub_dirs:
            directory_path = download_settings.base_dir.joinpath(directory)
            if directory_path.exists():
                print(f'The {directory_path} directory already exists!')
            else:
                print(f'Creating the {directory} directory!')
                directory_path.mkdir()

        # Download files from GDAC to Index directory
        index_directory = download_settings.base_dir.joinpath("Index")
        if index_directory: 
            for file in download_settings.index_files:
                # Check if the file we want to download is already at our savepoint
                    # If the file is already there check if we need to update it or not
                # If the file does not exist or we want to update the file then: 
                download_file(file, index_directory, source_settings.hosts)
        else: 
            print(f'The Index directory does not exist, we need the index directory to save the index files to.')

        # Fill in avail_vars variable in the SourceSettings class
        # Fill in dacs variable in the SourceSettings class

        # Extract Unique floats from both data frames
            # There is some post processing that they do on unique floats in the initalize_argo.m
            # his any of that still relevant? 

    
    def download_file(filename: str, savepoint: Path, hosts: list) -> None:
        """ A function to download and save a file from GDAC sources. 

            :param: filename : str - The name of the file we are downloading.
            :param: savepoint : Path - The directory that we are saving the file to.
            :param: hosts : list - A list of URLs to the GDAC sources. 
        """
        response = try_download()


    def try_download(url: str):
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
            return try_download(url)
        

