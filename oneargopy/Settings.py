# -*- coding: utf-8 -*-
#initialize.py
#------------------------------------------------------------------------------
# Created By: Savannah Stephenson
# Creation Date: 05/31/2024
# Version: 1.0
#------------------------------------------------------------------------------
""" This file holds the classes that the library will use to describe 
    various settings involved in the process of extracting and 
    plotting data from the Argo floats. All attributes will have a
    default value that is assigned upon use of the class that can be
    viewed and altered with setter and getter functions.  
"""
#------------------------------------------------------------------------------
# 
#
# Imports
# System
from pathlib import Path


class DownloadSettings():
    """ The DownloadSettings class is used to store all of the information
        needed in to create directories to store downloaded data from 
        the Global Data Assembly Center (GDAC), when to log downloads, and 
        when to update downloaded data.

        :param: base_dir : str - The base directory that all sub directories 
            should be created at.
        :param: sub_dirs : list - A list of folders to that will store 
            downloaded data.
        :parm: index_files : list - A list of the index files that will be 
            downloaded.
        :param: verbose : bool - A boolean value that determines weather to 
            log verbosely or not.
        :param: update : int - An integer value that determines the threshold
            for updating downloaded files (0: do not update; >0: maximum 
            number of seconds since an index file was downloaded before 
            downloading it again for new profile selection).
        :param: max_attempts : int - An integer value that determines the 
            number of times argo tries to download the same file before 
            raising an exception.
    """
    def __init__(self, 
                 base_dir: Path = None, 
                 sub_dirs: list = None,
                 index_files: list = None,
                 verbose: bool = True,
                 update: int = 3600,
                 max_attempts: int = 10) -> None:
        self.base_dir = base_dir if base_dir is not None else Path(__file__).resolve().parent
        self.sub_dirs = sub_dirs if sub_dirs is not None else ["Index", "Meta", "Tech", "Traj", "Profiles"]
        self.index_files = index_files if index_files is not None else ["ar_index_global_traj.txt", "ar_index_global_tech.txt", "ar_index_global_meta.txt", 
                                                                        "ar_index_global_prof.txt", "argo_synthetic-profile_index.txt"]
        self.verbose = verbose
        self.update = update
        self.max_attempts = max_attempts


    def __str__(self) -> str:
        return f'\n[Download Settings] -> Base Directory: {self.base_dir}, Sub Directories: {self.sub_dirs}, Index Files: {self.index_files}, Verbose Setting: {self.verbose}, Max Attempts: {self.max_attempts}'
    

    def __repr__(self) -> str:
        return f'\nDownloadSettings({self.base_dir}, {self.sub_dirs}, {self.index_files}, {self.verbose}, {self.update}, {self.max_attempts})'
    

    def __eq__(self, __value: object) -> bool:
        if (self.base_dir == __value.base_dir and 
            self.sub_dirs == __value.sub_dirs and
            self.index_files == __value.index_files and
            self.verbose == __value.verbose and
            self.update == __value.update and
            self.max_attempts == __value.max_attempts):
            return True
        else: return False


class AnalysisSettings():
    """ The AnalysisSettings class is used to store all of the default 
        settings for analyzing data from the argo floats.

        :param: temp_thresh : float - The temperature threshold for mixed 
            layer depth calculations measured in degrees Celsius. 
        :param: dens_thresh : float - The density threshold for mixed layer 
            depth calculations measured in kg/m^3.
        :param: interp_lonlat : bool - A boolean value determining weather 
            or not to interpolate missing latitude and longitude values
    """
    def __init__(self,
                 temp_thresh: float = 0.2,
                 dens_thresh: float = 0.03,
                 interp_lonlat: bool = False) -> None:
        self.temp_thresh = temp_thresh
        self.dens_thresh = dens_thresh
        self.interp_lonlat = interp_lonlat


    def __str__(self) -> str:
        return f'\n[Analysis Settings] -> Temperature Threshold: {self.temp_thresh}, Density Threshold: {self.dens_thresh}, Interpolate Latitude and Longitude: {self.interp_lonlat}'
    

    def __repr__(self) -> str:
        return f'\nAnalysisSettings({self.temp_thresh}, {self.dens_thresh}, {self.verbose}, {self.interp_lonlat})'
    

    def __eq__(self, __value: object) -> bool:
        if (self.temp_thresh == __value.temp_thresh and 
            self.dens_thresh == __value.dens_thresh and 
            self.interp_lonlat == __value.interp_lonlat):
            return True
        else: return False


class SourceSettings():
    """ The SourceSettings class is used to store information about where 
        we are collecting the Argo Float data from.

        :param: hosts : list - The US and French GDAC URLs. IFREMER is often
            faster than GODAE so it is listed first.
        :param: avail_vars : list - The full set of available variables, 
            will be filled during evaluation of the index files.
        :param: dacs : list - A list of Data Assimilation Centers, will be 
            fileld during evaluation of the index files. 
    """
    def __init__(self) -> None:
        self.hosts =  ["https://data-argo.ifremer.fr/", 
                       "https://usgodae.org/ftp/outgoing/argo/"]
        self.avail_vars =  None
        self.dacs =  None


    def __str__(self) -> str:
        return f'\n[Source Settings] -> Hosts: {self.hosts}, Available Variables: {self.avail_vars}, Data Assimilation Centers: {self.dacs}'
    

    def __repr__(self) -> str:
        return f'\nSourceSettings()'
    

    def __eq__(self, __value: object) -> bool:
        if (self.hosts == __value.hosts and 
            self.avail_vars == __value.avail_vars and 
            self.dacs == __value.dacs):
            return True
        else: return False