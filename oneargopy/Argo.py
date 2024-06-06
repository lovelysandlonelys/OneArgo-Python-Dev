# -*- coding: utf-8 -*-
# Argo.py
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
import pandas as pd
import time
import requests
from datetime import datetime
import shutil
import gzip

class Argo:
    
    def __init__(self, user_settings: str = None) -> None:
        """ The initialize function downloads the index files form GDAC and 
            stores them in the proper directories defined in the 
            DownloadSettings class. 

            :param: user_settings : str - An optional parameter that will be used
                to initialize the Settings classes if passed. Should be the 
        """
        print(f'Starting initialize process...\n')
        
        self.download_settings = DownloadSettings(user_settings)
        self.source_settings = SourceSettings(user_settings)
        
        print(f'Your current download settings are: {self.download_settings}')
        print(f'Your current source settings are: {self.source_settings}')

        # Check for and create subdirectories if needed
        print(f'Checking for subdirectories...\n')
        self.__initialize_subdirectories()

        # Download files from GDAC to Index directory
        print(f'\nDownloading index files...\n')
        for file in self.download_settings.index_files:
            self.__download_index_file(file)

        # Load the argo_synthetic-profile_index.txt file into a data frame
        print(f'\n Transfering index files into data frames...')
        self.synthetic_index = self.__load_dataframe('argo_synthetic-profile_index.txt')
        self.prof_index = self.__load_dataframe('ar_index_global_prof.txt')
        
        # Fill in avail_vars variable in the SourceSettings class
        self.source_settings.set_avail_vars(self.synthetic_index)

        # Fill in dacs variable in the SourceSettings class
        self.source_settings.set_dacs(self.synthetic_index)

        # Extract Unique floats from both data frames
            # There is some post processing that they do on unique floats in the initalize_argo.m
            # his any of that still relevant? 

        print(f'Initialize is finished!')


    def __initialize_subdirectories(self) -> None:
        """ A function that checks for and creates the necessary folders as 
            listed in the download settings sub_dir list. 
        """
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


    def __download_index_file(self, file_name: str) -> None:
        """ A function to download and save an index file from GDAC sources. 

            :param: filename : str - The name of the file we are downloading.
        """
        index_directory = Path(self.download_settings.base_dir.joinpath("Index"))

        # Get the expected filepath for the file
        file_path = index_directory.joinpath(file_name)

        # Check if the filepath exists
        if file_path.exists():

            # Check if the settings allow  for updates
            if self.download_settings.update == 0:
                print(f'The download settings have update set to 0, indicating that we do not want to update index files.')
            else: 
                txt_last_modified_time = Path(file_path).stat().st_mtime
                print(f'The last modified time .txt: {txt_last_modified_time}')
                
                current_time = datetime.now().timestamp()
                print(f'The current time: {current_time}')
                txt_seconds_since_modified = current_time - txt_last_modified_time
                print(f'The seconds since modified txt: {txt_seconds_since_modified}')
                print(f'The download threshold: {self.download_settings.update}')

                # Check if the file should be updated
                if (txt_seconds_since_modified > self.download_settings.update):
                    print(f'The file: {file_name} needs to be updated.')
                    self.__try_download(file_name ,True)
                else:
                    print(f'The file: {file_name} does not need to be updated yet.\n')

        # if the file doesn't exist then download it
        else: 
            print(f'The file: {file_name} needs to be downloaded.')
            self.__try_download(file_name, False)


    def __try_download(self, file_name: str, update_status: bool)-> None:
        """ A function that attempts to download a file from both GDAC sources.

            :param: file_name : str - The name of the file to download
            :param: update_status: bool - True if the file exists and we 
                are trying to update it. False if the file hasn't been 
                downloaded yet. 
        """
        index_directory = Path(self.download_settings.base_dir.joinpath("Index"))

        success = False
        iterations = 0
        txt_save_path = index_directory.joinpath(file_name)
        gz_save_path = index_directory.joinpath("".join([file_name, ".gz"]))

        while (not success) and (iterations < self.download_settings.max_attempts):
            # Try both hosts (perfered one is listed first in download settings)
            for host in self.source_settings.hosts:

                url = "".join([host, file_name, ".gz"])

                print(f'URL we are trying to download from: {url}')
                print(f'We are saving {file_name}.gz to {gz_save_path}')


                start_time = time.time()
                try:
                    with requests.get(url, stream=True) as r:
                        r.raise_for_status()
                        with open(gz_save_path, 'wb') as f:
                            r.raw.decode_content = True
                            shutil.copyfileobj(r.raw, f)
                    
                    download_time = time.time() - start_time

                    unzip_time_start = time.time()

                    print(f'Unzipping {file_name}.gz...')
                    with gzip.open(gz_save_path, 'rb') as gz_file:
                        with open(txt_save_path, 'wb') as txt_file:
                            shutil.copyfileobj(gz_file, txt_file)

                    unzip_time = time.time() - unzip_time_start
                    
                    success = True
                    print(f'{file_name}.gz was successfully downloaded and unzipped to {file_name}')
                    print(f'Download took {download_time:.2f} seconds.')
                    print(f'Unzip took {unzip_time:.2f} seconds.\n')

                    # Remove extraneous .gz file
                    gz_save_path.unlink()
                    
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
        

    def __load_dataframe(self, file_name: str) -> pd:
        """ A function to load an index file into a data frame for easier refrence.

            :param: file_name : str - The name of the file that we would like
                to read into a dataframe.

            Notes: the header is 8 here because there are 8 lines in both index fiels
                devoted to header information.
        """
        start_time = time.time()
        file_path = Path.joinpath(self.download_settings.base_dir, 'Index', file_name)
        dataframe = pd.read_csv(file_path, delimiter=',', header=8, parse_dates=['date','date_update'], 
                                date_format='%Y%m%d%H%M%S')
        print(dataframe)
        transfer_time = time.time() - start_time
        print(f'The transfer time for {file_name} was: {transfer_time}')
        return dataframe
