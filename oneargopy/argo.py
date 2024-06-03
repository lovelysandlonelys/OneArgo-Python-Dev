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

class argo():
    """ The argo class which contains functions for downloading and handling argo float data. 
    """


    def initialize():
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
            # Sprof
            # Prof
            # Meta
            # Tech
            # Traj

        # Fill in avail_vars variable in the SourceSettings class
        # Fill in dacs variable in the SourceSettings class

        # Extract Unique floats from both data frames
            # There is some post processing that they do on unique floats in the initalize_argo.m
            # his any of that still relevant? 
        pass
