# -*- coding: utf-8 -*-
#initialize.py
#----------------------------------
# Created By: Savannah Stephenson
# Creation Date: 05/31/2024
# Version: 1.0
#----------------------------------
""" This file holds the classes that the library will use to describe 
    various settings involved in the process of extracting and 
    plotting data from the Argo floats. All attributes will have a
    default value that is assigned upon use of the class that can be
    viewed and altered with setter and getter functions.  
"""
#----------------------------------
# 
#
# Imports
import os


class DownloadSettings():
    """ The DownloadSettings class is used to store all of the information
        needed in to create directories to store downloaded data from 
        the Global Data Assembly Center (GDAC), when to log downloads, and 
        when to update downloaded data.

        :param: base_dir : str - The base directory that all sub directories should be created at.
        :param: sub_dirs : list - A list of folders to that will store downloaded data.
        :param: verbose : bool - A boolean value that determines weather to log verbosely or not.
        :param: update : int - An integer value that determines the threshold for updating downloaded files (0: do not update; >0: maximum number of seconds since an index file was downloaded before downloading it again for new profile selection).
    """
    def __init__(self, 
                 base_dir: str = None, 
                 sub_dirs: list = None,
                 verbose: bool = True,
                 update: int = 3600) -> None:
        self.base_dir = base_dir if base_dir is not None else os.path.dirname(os.path.realpath(__file__))
        self.sub_dirs = sub_dirs if sub_dirs is not None else ["/Index", "/Meta", "/Tech", "/Traj", "/Profiles"]
        self.verbose = verbose
        self.update = update


    def __str__(self) -> str:
        return f'\n[Download Settings] -> Base Directory: {self.base_dir}, Sub Directories: {self.sub_dirs}, Verbose Setting: {self.verbose}'
    

    def __repr__(self) -> str:
        return f'\nDownloadSettings({self.base_dir}, {self.sub_dirs}, {self.verbose}, {self.update})'
    

    def __eq__(self, __value: object) -> bool:
        if (self.base_dir == __value.base_dir and 
            self.sub_dirs == __value.sub_dirs and 
            self.verbose == __value.verbose and
            self.update == __value.update):
            return True
        else: return False


class AnalysisSettings():
    """ The AnalysisSettings class is used to store all of the default settings for analyzing
        data from the argo floats.

        :param: temp_thresh : float - The temperature threshold for mixed layer depth calculations measured in degrees Celsius. 
        :param: dens_thresh : float - The density threshold for mixed layer depth calculations measured in kg/m^3.
        :param: interp_lonlat : bool - A boolean value determining weather or not to interpolate missing latitude and longitude values
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
    """ The SourceSettings class is used to store information about where we are collecting the Argo Float data from.

        :param: hosts : list - The US and French GDAC URLs. IFREMER is often faster than GODAE so it is listed first.
        :param: avail_vars : list - The full set of available variables, will be filled during evaluation of the index files.
        :param: dacs : list - A list of Data Assimilation Centers, will be fileld during evaluation of the index files. 
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