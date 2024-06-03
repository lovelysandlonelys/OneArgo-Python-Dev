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
#Imports
from Settings import DownloadSettings, SourceSettings


class argo():
    """ The argo class which contains functions for downloading and handling argo float data. 
    """


    def initialize():
        """ The initialize function 
        """
        print("testing\n")
        # Check for and create subdirectories if needed
        # Show current download settings. 

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
