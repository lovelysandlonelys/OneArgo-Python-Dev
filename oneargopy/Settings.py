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
        GDAC, when to log downloads, and when to update downloaded data.

        :param: base_dir : str - The base directory that all sub directories should be created at.
        :param: sub_dirs : list - A list of folders to that will store downloaded data.
        :param: verbose : bool - A boolean value that determines weather to log verbosely or not.
        :param: update : int - An integer value that determines the threshold for updating downloaded files.
    """
    def __init__(self, 
                 base_dir: str = None, 
                 sub_dirs: list = None,
                 verbose: bool = True,
                 update: int = 3600) -> None:
        self.base_dir = base_dir if base_dir is not None else os.path.dirname(os.path.realpath(__file__))
        self.sub_dirs = sub_dirs if sub_dirs is not None else ["/Index/", "/Meta/", "/Tech/", "/Traj/"]
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

        :param: temp_thresh : int -
        :param: dens_thresh : int -
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
        return f'\n[Analysis Settings] -> Temp Thresh: {self.temp_thresh}, Dense Thresh: {self.dens_thresh}, Interpolate Latitude and Longitude: {self.interp_lonlat}'
    

    def __repr__(self) -> str:
        return f'\nAnalysisSettings({self.temp_thresh}, {self.dens_thresh}, {self.verbose}, {self.interp_lonlat})'
    

    def __eq__(self, __value: object) -> bool:
        if (self.temp_thresh == __value.temp_thresh and 
            self.dens_thresh == __value.dens_thresh and 
            self.interp_lonlat == __value.interp_lonlat):
            return True
        else: return False


class PlotSettings():
    """ The PlotSettings class is used to store settings for plotting data. 

        NOTE: holding out on filling out, I want to cross reference the python versions that have been made to see if we still need
        a lot of these values. 
    """
    pass

class SourceSettings():
    """ The SourceSettings class is used to store information about where we are collecting the Argo Float data from.

        :param: hosts : list - The US and French GDAC URLs. IFREMER is often faster than GODAE so it is listed first.
        :param: avail_vars : list - The full set of available variables
        :param: dacs : list - list of Data Assimilation Centers
    """
    def __init__(self) -> None:
        self.hosts =  ["https://data-argo.ifremer.fr/", 
                       "https://usgodae.org/ftp/outgoing/argo/"]

        self.avail_vars =  ['PRES','PSAL','TEMP','CNDC','DOXY','BBP','BBP470',
                            'BBP532','BBP700','TURBIDITY','CP','CP660','CHLA','CDOM','NITRATE',
                            'BISULFIDE','PH_IN_SITU_TOTAL','DOWN_IRRADIANCE','DOWN_IRRADIANCE380',
                            'DOWN_IRRADIANCE412','DOWN_IRRADIANCE443','DOWN_IRRADIANCE490',
                            'DOWN_IRRADIANCE555','DOWN_IRRADIANCE670','DOWNWELLING_PAR',
                            'UP_RADIANCE','UP_RADIANCE412','UP_RADIANCE443','UP_RADIANCE490',
                            'UP_RADIANCE555']

        self.dacs =  ['aoml', 'bodc', 'coriolis', 'csio', 'csiro',
                      'incois', 'jma', 'kma', 'kordi', 'meds']


    def __str__(self) -> str:
        return f'\n[Analysis Settings] -> Temp Thresh: {self.hosts}, Dense Thresh: {self.avail_vars}, Interpolate Latitude and Longitude: {self.dacs}'
    

    def __repr__(self) -> str:
        return f'\nAnalysisSettings({self.hosts}, {self.avail_vars}, {self.verbose}, {self.dacs})'
    

    def __eq__(self, __value: object) -> bool:
        if (self.hosts == __value.hosts and 
            self.avail_vars == __value.avail_vars and 
            self.dacs == __value.dacs):
            return True
        else: return False