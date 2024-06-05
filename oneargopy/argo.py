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
from setup_functions import initialize_subdirectories, download_index_files

def initialize(user_settings: str = None) -> None:
    """ The initialize function downloads the index files form GDAC and 
        stores them in the proper directories defined in the 
        DownloadSettings class. 
    """
    print(f'Starting initialize process...\n')
    if user_settings:
        download_settings = DownloadSettings()
        source_settings = SourceSettings()
    else: 
        download_settings = DownloadSettings()
        source_settings = SourceSettings()
    
    print(f'Your current download settings are: {download_settings}')
    print(f'Your current source settings are: {source_settings}')

    # Check for and create subdirectories if needed
    print(f'Checking for subdirectories...\n')
    initialize_subdirectories(download_settings)

    # Download files from GDAC to Index directory
    print(f'\nDownloading index files...\n')
    for file in download_settings.index_files:
        download_index_files(file, download_settings, source_settings)
    
    # Fill in avail_vars variable in the SourceSettings class
    source_settings.set_avail_vars()

    # Fill in dacs variable in the SourceSettings class
    source_settings.set_dacs()

    # Extract Unique floats from both data frames
        # There is some post processing that they do on unique floats in the initalize_argo.m
        # his any of that still relevant? 

    print(f'Initialize is finished!')
    