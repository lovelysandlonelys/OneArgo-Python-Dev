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
from setup_functions import initialize_subdirectories, download_index_files, parse_settings

# System
from pathlib import Path
import pandas as pd

def initialize(user_settings: str = None) -> None:
    """ The initialize function downloads the index files form GDAC and 
        stores them in the proper directories defined in the 
        DownloadSettings class. 

        :param: user_settings : str - An optional parameter that will be used
            to initialize the Settings classes if passed. Should be the 
    """
    print(f'Starting initialize process...\n')
     
    download_settings = DownloadSettings()
    source_settings = SourceSettings()

    if user_settings:
        download_settings, source_settings = parse_settings(user_settings)
    
    print(f'Your current download settings are: {download_settings}')
    print(f'Your current source settings are: {source_settings}')

    # Check for and create subdirectories if needed
    print(f'Checking for subdirectories...\n')
    initialize_subdirectories(download_settings)

    # Download files from GDAC to Index directory
    print(f'\nDownloading index files...\n')
    for file in download_settings.index_files:
        download_index_files(file, download_settings, source_settings)

    # Load the argo_synthetic-profile_index.txt file into a data frame
    synthetic_index_file_path = Path.joinpath(download_settings.base_dir, 'Index', 'argo_synthetic-profile_index.txt')
    synthetic_index = pd.read_csv(synthetic_index_file_path, delimiter=',', header=8, parse_dates=['date','date_update'], 
                             date_format='%Y%m%d%H%M%S')
    print(synthetic_index)

    # Load the ar_index_global_prof.txt file into a data frame
    prof_index_file_path = Path.joinpath(download_settings.base_dir, 'Index', 'argo_synthetic-profile_index.txt')
    prof_index = pd.read_csv(prof_index_file_path, delimiter=',', header=8, parse_dates=['date','date_update'], 
                             date_format='%Y%m%d%H%M%S')
    print(prof_index)
    
    # Fill in avail_vars variable in the SourceSettings class
    source_settings.set_avail_vars(synthetic_index)

    # Fill in dacs variable in the SourceSettings class
    source_settings.set_dacs(synthetic_index)

    # Extract Unique floats from both data frames
        # There is some post processing that they do on unique floats in the initalize_argo.m
        # his any of that still relevant? 

    print(f'Initialize is finished!')
    