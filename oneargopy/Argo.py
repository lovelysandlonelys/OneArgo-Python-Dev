# -*- coding: utf-8 -*-
# Argo.py
#------------------------------------------------------------------------------
# Created By: Savannah Stephenson
# Creation Date: 05/30/2024
# Version: 1.0
#------------------------------------------------------------------------------
""" The Argo class contains the primary functions for downloading and handling
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
import requests
from datetime import datetime, timedelta, timezone
import shutil
import gzip
import numpy as np
import matplotlib.path as mpltPath
import pandas as pd

from time import time
import json


class Argo:
    
    def __init__(self, user_settings: str = None) -> None:
        """ The Argo constructor downloads the index files form GDAC and 
            stores them in the proper directories defined in the 
            DownloadSettings class. It then constructs thee dataframes
            from the argo_synthetic-profile_index.txt file and the
            ar_index_global_prof.txt file for use in class function
            calls. Two of the dataframes are a reflection of the index
            files, the third dataframe is a two column frame with 
            float ids and if they are a bgc float or not. 

            :param: user_settings : str - An optional parameter that will be used
                to initialize the Settings classes if passed. Should be the 
                full filepath. 

            NOTE: If the user has their own settings configuration and has
            set keep_index_in_memory to false then the dataframes will be
            removed from memory at the end of construction and will be
            reloaded with following Argo function calls, meaning that
            functions will take longer but occupy less memory if this
            option is set to false. 
        """
        self.download_settings = DownloadSettings(user_settings)
        self.source_settings = SourceSettings(user_settings)
        if self.download_settings.verbose: print(f'Starting initialize process...')
        
        if self.download_settings.verbose: print(f'Your current download settings are: {self.download_settings}')
        if self.download_settings.verbose: print(f'Your current source settings are: {self.source_settings}')

        # Check for and create subdirectories if needed
        if self.download_settings.verbose: print(f'Checking for subdirectories...')
        self.__initialize_subdirectories()

        # Download files from GDAC to Index directory
        if self.download_settings.verbose: print(f'\nDownloading index files...')
        for file in self.download_settings.index_files:
            self.__download_index_file(file)

        # Load the index files into dataframes
        if self.download_settings.verbose: print(f'\nTransferring index files into data frames...')
        self.sprof_index  = self.__load_sprof_dataframe() 
        self.prof_index = self.__load_prof_dataframe()

        # Add column noting if a float profile is also in the sprof_index, meaning that it is a bgc float
        if self.download_settings.verbose: print(f'Marking bgc floats in prof_index dataframe...')
        self.__mark_bgcs_in_prof()

        # Create float_is_bgc refrence index for use in select profiles
        if self.download_settings.verbose: print(f'Creating float_is_bgc_index dataframe...')
        self.float_is_bgc_index = self.__load_is_bgc_index()
        
        # Print number of floats
        if self.download_settings.verbose: self.__display_floats() 

        if self.download_settings.verbose: print(f'Initialize is finished!\n\n')

        if not self.download_settings.keep_index_in_memory:
            if self.download_settings.verbose: print('Removing dataframes from memory...')
            del self.sprof_index 
            del self.prof_index
            del self.float_is_bgc_index


    def select_profiles(self, lon_lim: list = [-180, 180], lat_lim: list = [-90, 90], start_date: str = '1995-01-01', end_date: str = None, **kwargs)-> dict:
        """ select_profiles is a public function of the Argo class that returns a 
            dictionary if float IDs and profile lists that match the passed criteria.

            :param: lon_lim : list - Longitude limits
            :param: lat_lim : list - Latitude limits
            :param: start_date : str - A UTC date in YYYY-MM-DD format.
            :param: end_date : str - An optional UTC date in YYYY-MM-DD format.
            :param: kargs : keyvalue arguments - Optional key argument values for
                further filtering of the float profiles returned by the function.

            :return: narrowed_profiles : dict - A dictionary with float ID
                keys corresponding to a list of profiles that match criteria.
                
            NOTE: 
            The longitude and latitude limits can be entered as either
            two element lists, in which case the limits will be interpreted
            as maximum and minimum limits tht form a rectangle, or they
            can be entered as a longer list in which case each pair of longitude
            and latitude values correspond to a vertices of a polygon.
            The longitude and latitude limits can be input in any 360 degree
            range that encloses all the desired longitude values.

            KEY ARGUMENT VALUE OPTIONS IN PROGRESS:
            floats=floats[] or 'float': Select profiles only from these floats that must
                    match all other criteria
            ocean=ocean: Valid choices are 'A' (Atlantic), 'P' (Pacific), and
                    'I' (Indian). This selection is in addition to the specified
                    longitude and latitude limits. (To select all floats and
                    profiles from one ocean basin, leave lon_lim and lat_lim
                    empty.)
            outside='none' or 'time' or 'space' or'both': By default, only float profiles
                    that are within both the temporal and spatial constraints are
                    returned ('none'); specify to also maintain profiles outside
                    the temporal constraints ('time'), spatial constraints
                    ('space'), or both constraints ('both')
            type', type: Valid choices are 'bgc' (select BGC floats only),
                    'phys' (select core and deep floats only),
                    and 'all' (select all floats that match other criteria).
                    If type is not specified, but sensors are, then the type will
                    be set to 'bgc' if sensors other than PRES, PSAL, TEMP, or CNDC
                    are specified.
                    In all other cases the default type is DownloadSettings.float_type,
                    which is set in the Argo constructor, you can also set the float_type
                    as a different value if passing a configuration file to the Argo constructor.

            would like to implement before end of project/easier ones
            sensor='sensor' or [sensors], SENSOR_TYPE: This option allows the selection by
                    sensor type. Available as of 2024: PRES, PSAL, TEMP, DOXY, BBP,
                    BBP470, BBP532, BBP700, TURBIDITY, CP, CP660, CHLA, CDOM,
                    NITRATE, BISULFIDE, PH_IN_SITU_TOTAL, DOWN_IRRADIANCE,
                    DOWN_IRRADIANCE380, DOWN_IRRADIANCE412, DOWN_IRRADIANCE443,
                    DOWN_IRRADIANCE490, DOWN_IRRADIANCE555, DOWN_IRRADIANCE670,
                    UP_RADIANCE, UP_RADIANCE412, UP_RADIANCE443, UP_RADIANCE490,
                    UP_RADIANCE555, DOWNWELLING_PAR, CNDC, DOXY2, DOXY3, BBP700_2
                    Multiple sensors can be entered as a list, e.g.: ['DOXY';'NITRATE']
            dac=dac: Select by Data Assimilation Center responsible for the floats.
                    A single DAC can be entered as a string (e.g.: 'aoml'),
                    multiple DACs can be entered as a list of strings (e.g.:
                    ['meds';'incois'].
                    Valid values as of 2024 are any: {'aoml'; 'bodc'; 'coriolis'; ...
                    'csio'; 'csiro'; 'incois'; 'jma'; 'kma'; 'kordi'; 'meds'}
        """
        if self.download_settings.verbose: print(f'Starting select_profiles...')
        self.epsilon = 1e-3
        self.lon_lim = lon_lim
        self.lat_lim = lat_lim
        self.start_date = start_date
        self.end_date = end_date
        self.outside = kwargs.get('outside')
        self.float_type = kwargs.get('type') if kwargs.get('type') is not None else self.download_settings.float_type
        self.floats = kwargs.get('floats')
        self.ocean = kwargs.get('ocean')
        self.sensor = kwargs.get('sensor')

        if self.download_settings.verbose: print(f'Validating parameters...')
        self.__validate_lon_lat_limits()
        self.__validate_start_end_dates()
        if self.outside : self.__validate_outside_kwarg()
        if self.float_type : self.__validate_type_kwarg()
        if self.floats : self.__validate_floats_kwarg()
        if self.ocean : self.__validate_ocean_kwarg()
        # if self.sensor : self.__validate_sensor_kwarg()

        # Load correct dataframes according to self.float_type and self.floats
        # we set self.selected_from_sprof_index and self.selected_from_prof_index
        # in this function which will be used in __narrow_profiles_by_criteria
        self.__prepare_selection()
        
        # Narrow down float profiles and save in dictionary
        narrowed_profiles = self.__narrow_profiles_by_criteria()

        if not self.download_settings.keep_index_in_memory:
            if self.download_settings.verbose: print('Removing dataframes from memory...')
            del self.sprof_index 
            del self.prof_index
            del self.selection_frame

        if self.download_settings.verbose: print(f'Profiles Selected!\n\n')

        return narrowed_profiles


    def __initialize_subdirectories(self) -> None:
        """ A function that checks for and creates the necessary folders as 
            listed in the download settings sub_dir list. 
        """
        for directory in self.download_settings.sub_dirs:
            directory_path = self.download_settings.base_dir.joinpath(directory)
            if directory_path.exists():
                if self.download_settings.verbose: print(f'The {directory_path} directory already exists!')
            else:
                try:
                    if self.download_settings.verbose: print(f'Creating the {directory} directory!')
                    directory_path.mkdir()
                except Exception as e:
                    if self.download_settings.verbose: print(f'Failed to create the {directory} directory: {e}')


    def __download_index_file(self, file_name: str) -> None:
        """ A function to download and save an index file from GDAC sources. 

            :param: filename : str - The name of the file we are downloading.
        """
        index_directory = Path(self.download_settings.base_dir.joinpath("Index"))

        # Get the expected filepath for the file
        file_path = index_directory.joinpath(file_name)

        # Check if the filepath exists
        if file_path.exists():

            # Check if the settings allow for updates
            if self.download_settings.update == 0:
                if self.download_settings.verbose: 
                    print(f'The download settings have update set to 0, indicating that we do not want to update index files.')
            else: 
                txt_last_modified_time = Path(file_path).stat().st_mtime
                current_time = datetime.now().timestamp()
                txt_seconds_since_modified = current_time - txt_last_modified_time
                # Check if the file should be updated
                if (txt_seconds_since_modified > self.download_settings.update):
                    if self.download_settings.verbose: print(f'Updating {file_name}...')
                    self.__try_download(file_name ,True)
                else:
                    if self.download_settings.verbose: print(f'{file_name} does not need to be updated yet.')

        # if the file doesn't exist then download it
        else: 
            if self.download_settings.verbose: print(f'{file_name} needs to be downloaded.')
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
            # Try both hosts (preferred one is listed first in download settings)
            for host in self.source_settings.hosts:

                url = "".join([host, file_name, ".gz"])

                if self.download_settings.verbose: print(f'Downloading {file_name} from {url}...')

                try:
                    with requests.get(url, stream=True) as r:
                        r.raise_for_status()
                        with open(gz_save_path, 'wb') as f:
                            r.raw.decode_content = True
                            shutil.copyfileobj(r.raw, f)

                    if self.download_settings.verbose: print(f'Unzipping {file_name}.gz...')
                    with gzip.open(gz_save_path, 'rb') as gz_file:
                        with open(txt_save_path, 'wb') as txt_file:
                            shutil.copyfileobj(gz_file, txt_file)
                    
                    success = True
                    if self.download_settings.verbose: print(f'Success!')

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
            if update_status:
                print(f'WARNING: Update for {file_name} failed, you are working with out of date data.')
            else:
                raise Exception(f'Download failed! {file_name} could not be downloaded at this time.')
        

    def __load_sprof_dataframe(self) -> pd:
        """ A function to load the sprof index file into a data frame for easier reference.
        """
        file_name = "argo_synthetic-profile_index.txt"
        # The header is 8 here because there are 8 lines in both index files devoted to header information.
        file_path = Path.joinpath(self.download_settings.base_dir, 'Index', file_name)
        sprof_index  = pd.read_csv(file_path, delimiter=',', header=8, parse_dates=['date','date_update'], 
                                date_format='%Y%m%d%H%M%S')
        
        # Parsing out variables in first column: file
        dacs = sprof_index ['file'].str.split('/').str[0]
        sprof_index.insert(1, "dacs", dacs)

        wmoid = sprof_index ['file'].str.split('/').str[1].astype('int')
        sprof_index.insert(0, "wmoid", wmoid)

        profile = sprof_index ['file'].str.split('_').str[1].str.replace('.nc', '')
        sprof_index.insert(2, "profile", profile)

        # Splitting the parameters into their own columns
        parameters_split = sprof_index ['parameters'].str.split()
        data_types_split = sprof_index ['parameter_data_mode'].apply(list)

        # R: raw data, A: adjusted mode (real-time adjusted), D: delayed mode quality controlled
        data_type_mapping = {np.nan: 0, 'R':1, 'A':2, 'D':3 }
        mapped_data_types_split = data_types_split.apply(lambda lst: [data_type_mapping.get(x, 0) if pd.notna(x) else 0 for x in lst])

        # Create a new DataFrame from the split parameters 
        expanded_df = pd.DataFrame({
            'index': sprof_index .index.repeat(parameters_split.str.len()),
            'parameter': parameters_split.explode(),
            'data_type': mapped_data_types_split.explode()
        })

        # Pivot the expanded DataFrame to get parameters as columns
        with pd.option_context('future.no_silent_downcasting', True):
            result_df = expanded_df.pivot(index='index', columns='parameter', values='data_type').fillna(0).astype('int8')

        # Fill in source_settings information based off of synthetic file before removing rows
        if self.download_settings.verbose: print(f'Filling in source settings information...')
        self.source_settings.set_avail_vars(sprof_index )
        self.source_settings.set_dacs(sprof_index )

        # Merge the pivoted DataFrame back with the original DataFrame and drop split rows
        if self.download_settings.verbose: print(f'Marking Parameters with their data mode...')
        sprof_index = sprof_index .drop(columns=['parameters', 'parameter_data_mode'])
        sprof_index = sprof_index .join(result_df)

        # Add profile_index column
        sprof_index.sort_values(by=['wmoid', 'date'], inplace=True)
        sprof_index.insert(0, "profile_index", 0)
        sprof_index['profile_index'] = sprof_index.groupby('wmoid')['date'].cumcount() + 1

        return sprof_index 
        

    def __load_prof_dataframe(self) -> pd:
        """ A function to load the prof index file into a data frame for easier reference.
        """
        file_name = "ar_index_global_prof.txt"
        file_path = Path.joinpath(self.download_settings.base_dir, 'Index', file_name)
        # The header is 8 here because there are 8 lines in both index files devoted to header information.
        prof_index = pd.read_csv(file_path, delimiter=',', header=8, parse_dates=['date','date_update'], 
                                date_format='%Y%m%d%H%M%S')
        
        # Splitting up parts of the first column
        dacs = prof_index['file'].str.split('/').str[0]
        prof_index.insert(0, "dacs", dacs)

        wmoid = prof_index['file'].str.split('/').str[1].astype('int')
        prof_index.insert(1, "wmoid", wmoid)

        R_file = prof_index['file'].str.split('/').str[3].str.startswith('R')
        prof_index.insert(2, "R_file", R_file)

        # Add profile_index column
        prof_index.sort_values(by=['wmoid', 'date'], inplace=True)
        prof_index.insert(0, "profile_index", 0)
        prof_index['profile_index'] = prof_index.groupby('wmoid')['date'].cumcount() + 1

        return prof_index
    

    def __mark_bgcs_in_prof(self):
        """ A function to mark weather the floats listed in prof_index are 
            biogeochemical floats or not. 
        """
        bgc_floats = self.sprof_index['wmoid'].unique()
        is_bgc = self.prof_index['wmoid'].isin(bgc_floats)
        self.prof_index.insert(1, "is_bgc", is_bgc)

    def __load_is_bgc_index(self)-> pd:
        """ Function to create a dataframe with float IDs and
            their is_bcg status for use in select_profiles().
        """ 
        float_bgc_status = self.prof_index[['wmoid', 'is_bgc']].drop_duplicates()
        return float_bgc_status


    def __display_floats(self) -> None:
        """ A function to display information about the number of floats initially 
            observed in the unfiltered dataframes. 
        """
        floats = self.prof_index['wmoid'].unique()
        profiles = self.prof_index['file'].unique()
        print(f"\n{len(floats)} floats with {len(profiles)} profiles found.")

        bgc_floats = self.sprof_index['wmoid'].unique()
        profiles = self.sprof_index['file'].unique()
        print(f"{len(bgc_floats)} BGC floats with {len(profiles)} profiles found.")


    def __validate_lon_lat_limits(self)-> None:
        """ Function to validate the length, order, and contents of longitude and latitude limits passed 
            to select_profiles.
        """
        if self.download_settings.verbose: print(f'Validating longitude and latitude limits...')

        # Validating Lists
        if len(self.lon_lim) != len(self.lat_lim):
            raise Exception(f'The length of the longitude and latitude lists must be equal.')
        if len(self.lon_lim) == 2:
            if (self.lon_lim[1] <= self.lon_lim[0]) or (self.lat_lim[1] <= self.lat_lim[0]):
                if self.download_settings.verbose: print(f'Longitude Limits: min={self.lon_lim[0]} max={self.lon_lim[1]}')
                if self.download_settings.verbose: print(f'Latitude Limits: min={self.lat_lim[0]} max={self.lat_lim[1]}')
                raise Exception(f'When passing longitude and latitude lists using the [min, max] format the max value must be greater than the min value.')
            if (abs(self.lon_lim[1]) - self.lon_lim[0] < self.epsilon) and (abs(self.lat_lim[1]) - self.lat_lim[0] < self.epsilon): 
                self.keep_full_geographic = True
            else: 
                self.keep_full_geographic = False

        # Validating latitudes
        if not all(-90 <= lat <= 90 for lat in self.lat_lim):
            print(f'Latitudes: {self.lat_lim}')
            raise Exception(f'Latitude values should be between -90 and 90.')
        
        # Validate Longitudes
        ## Checking range of longitude values
        lon_range = max(self.lon_lim) - min(self.lon_lim)
        if lon_range > 360 or lon_range <= 0:
            if self.download_settings.verbose: print(f'Current longitude range: {lon_range}')
            raise Exception(f'The range between the maximum and minimum longitude values must be between 1 and 360.')
        ## Adjusting values to fit between -180 and 360
        if  min(self.lon_lim) < -180:
            if self.download_settings.verbose: print(f'Adjusting within -180')
            self.lon_lim = [lon + 360.00 for lon in self.lon_lim]


    def __validate_start_end_dates(self):
        """ A function to validate the start and end date strings passed to select_profiles and
            converts them to datetimes for easier comparison to dataframe values later on.
        """
        if self.download_settings.verbose: print(f'Validating start and end dates...')
        
        # Parse Strings to Datetime Objects
        try:
            # Check if the string matches the expected format
            self.start_date = datetime.fromisoformat(self.start_date).replace(tzinfo=timezone.utc)
            # end_date is optional and should be set to tomorrow if not provided
            if self.end_date != None:
                self.end_date = datetime.fromisoformat(self.end_date).replace(tzinfo=timezone.utc)
            else:
                self.end_date = datetime.now(timezone.utc) + timedelta(days=1)
        except ValueError:
            print(f" Start date: {self.start_date} or end date: {self.end_date} is not in the expected format 'yyyy-mm-dd'")
        
        # Validate datetimes
        if self.start_date > self.end_date:
            if self.download_settings.verbose: 
                print(f'Current start date: {self.start_date}')
                print(f'Current end date: {self.end_date}')
            raise ValueError(f'The start date must be in the past when compared to the end date.')
        if self.start_date < datetime(1995, 1, 1, tzinfo=timezone.utc):
            if self.download_settings.verbose: print(f'Current start date: {self.start_date}')
            raise ValueError(f'Start date must be after at least: {datetime(1995, 1, 1, tzinfo=timezone.utc)} for any floats to be active.')
        
        # Set to datetime64 for dataframe comparisons
        self.start_date = np.datetime64(self.start_date)
        self.end_date = np.datetime64(self.end_date)


    def __validate_outside_kwarg(self): 
        """ A function to validate the value of the optional 'outside' keyword argument.
        """
        if self.download_settings.verbose: print(f"Validating 'outside' keyword argument...")

        if self.outside is not None: 
            if self.outside != 'time' and self.outside != 'space' and self.outside != 'both':
                raise Exception(f"The only acceptable values for the 'outside' keyword argument are 'time', 'space', and 'both'.")
            
    
    def __validate_type_kwarg(self): 
        """ A function to validate the value of the optional 'type' keyword argument.
        """
        if self.download_settings.verbose: print(f"Validating 'type' keyword argument...")

        if self.float_type != 'all' and self.float_type != 'phys' and self.float_type != 'bgc':
                raise Exception(f"The only acceptable values for the 'type' keyword argument are 'all', 'phys', and 'bgc'.")
        
    
    def __validate_floats_kwarg(self):
        """ A function to validated the 'floats' keyword argument. The 'floats' must be a list even if it is a single float.
        """
        if not isinstance(self.floats, list):
            self.floats = [self.floats]
        
    
    def __validate_ocean_kwarg(self): 
        """ A function to validate the value of the optional 'ocean' keyword argument.
        """
        if self.download_settings.verbose: print(f"Validating 'ocean' keyword argument...")

        if self.ocean != 'A' and self.ocean != 'P' and self.ocean != 'I':
                raise Exception(f"The only acceptable values for the 'ocean' keyword argument are 'A' (Atlantic), 'P' (Pacific), and 'I' (Indian).")


    def __prepare_selection(self):
        """ A function that determines what dataframes will be loaded/used when selecting floats.
            We determine what dataframes to load based on two factors: type and passed floats.  

            If type is 'phys', the dataframe based on ar_index_global_prof.txt will be used.
            If type is 'bgc', the dataframe based on argo_synthetic-profile_index.txt will be used.
            If type is 'all', both dataframes are used.

            If the user passed floats, we only load the passed floats into the selection frames.

            If keep_index_in_memory is set to false the dataframes created during Argo's 
            constructor are deleted. In this function we only reload the necessary
            dataframes into memory.
        """
        if self.download_settings.verbose: print(f'Preparing float data for filtering...')
        selected_floats_phys = None
        selected_floats_bgc = None

        # Load dataframes into memory if they are not there
        if not self.download_settings.keep_index_in_memory:
            self.sprof_index = self.__load_sprof_dataframe()
            self.prof_index = self.__load_prof_dataframe()
            self.float_is_bgc_index = self.__load_is_bgc_index()

        # If we aren't filtering from specific floats assign selected frames
        # to the whole index frames
        if self.floats is None: 
            self.selected_from_prof_index = self.prof_index[self.prof_index['is_bgc'] == False]
            self.selected_from_sprof_index = self.sprof_index

        # If we do have specific floats to filter from, assign 
        # selected floats by pulling those floats from the 
        # larger dataframes, only adding floats that match the 
        # type to the frames. 
        else:
            if self.float_type != 'phys':
                  # Make a list of bgc floats that the user wants 
                  bgc_filter = (self.float_is_bgc_index['wmoid'].isin(self.floats)) & (self.float_is_bgc_index['is_bgc'] == True)
                  selected_floats_bgc = self.float_is_bgc_index[bgc_filter]['wmoid'].tolist()
                  # Gather bgc profiles for these floats from sprof index frame
                  self.selected_from_sprof_index = self.sprof_index[self.sprof_index['wmoid'].isin(selected_floats_bgc)]
            if self.float_type != 'bgc': 
                  # Make a list of phys floats that the user wants 
                  phys_filter = (self.float_is_bgc_index['wmoid'].isin(self.floats)) & (self.float_is_bgc_index['is_bgc'] == False)
                  selected_floats_phys = self.float_is_bgc_index[phys_filter]['wmoid'].tolist()
                  # Gather phys profiles for these floats from prof index frame
                  self.selected_from_prof_index = self.prof_index[self.prof_index['wmoid'].isin(selected_floats_phys)]

        if self.download_settings.verbose:
            num_unique_floats = len(self.selected_from_sprof_index['wmoid'].unique()) + len(self.selected_from_prof_index['wmoid'].unique())
            print(f"We will filter through {num_unique_floats} floats!") 
            print(f'There are {len(self.selected_from_sprof_index) + len(self.selected_from_prof_index)} profiles are associated with these floats!')



    def __narrow_profiles_by_criteria(self)-> dict:
        """ A function to narrow down the available profiles to only those
            that meet the criteria passed to select_profiles.

            :return: narrowed_profiles : dict - A dictionary with float ID
                keys corresponding to a list of profiles that match criteria.
        """
        # Filter by time, space, and type constraints first.
        if self.float_type == 'bgc' : 
            self.selection_frame_phys = pd.DataFrame()
        else :
            self.selection_frame_phys = self.__get_in_time_and_space_constraints(self.selected_from_prof_index)
        if self.float_type == 'phys' : 
            self.selection_frame_bgc = pd.DataFrame()
        else :
            self.selection_frame_bgc = self.__get_in_time_and_space_constraints(self.selected_from_sprof_index)
        
        # Set the selection frame
        self.selection_frame = pd.concat([self.selection_frame_bgc, self.selection_frame_phys])

        # Remove extraneous frames if keep index in memory is set to false
        if not self.download_settings.keep_index_in_memory: 
            del self.sprof_index
            del self.prof_index
            del self.selection_frame_bgc
            del self.selection_frame_phys

        if self.download_settings.verbose:
            print(f"{len(self.selection_frame['wmoid'].unique())} floats selected!")   
            print(f'{len(self.selection_frame)} profiles selected according to time and space constraints!')
        
        # Filter by other constraints, these functions will use self.selection_frame 
        # so we don't have to pass a frame
        if self.ocean : self.__get_in_ocean_basin()
        # other narrowing functions that act on created selection frame...

        # Convert the working dataframe into a dictionary
        selected_floats_dict = self.__dataframe_to_dictionary()

        # Printing Dict, likely to remove after testing period
        print(f'Floats: {selected_floats_dict.keys()}')
        for key, value in selected_floats_dict.items():
            print(f'{key}: {value}')
        

    def __get_in_geographic_range(self, dataframe_to_filter: pd)-> list:
        """ A function to create and return two true false arrays indicating
            floats and profiles that fall within the geographic range.
        """
        # If the user has passed us the entire globe don't go through the whole
        # process of checking if the points of all the floats are inside the polygon
        if self.keep_full_geographic: 
            return  [True] * len(dataframe_to_filter)
        
        if self.download_settings.verbose: print(f'Sorting floats for those within the geographic range...')

        # Make points out of profile lat and lons
        if self.download_settings.verbose: print(f'Creating point list from profiles...')
        profile_points = np.empty((len(dataframe_to_filter), 2))
        
        # The longitudes in the dataframe are standardized to fall within -180 and 180.
        # but our longitudes only have a standard minimum value of -180. In this section
        # we adjust the longitude and latitudes in the dataframe to follow this minimum 
        # only approach.
        if max(self.lon_lim) > 180:
            if self.download_settings.verbose: print(f'The max value in lon_lim is {max(self.lon_lim)}')
            if self.download_settings.verbose: print(f'Adjusting longitude values...')
            profile_points[:,0] = dataframe_to_filter['longitude'].apply(lambda x: x + 360 if -180 < x < min(self.lon_lim) else x).values
        else:
            profile_points[:,0] = dataframe_to_filter['longitude'].values
        
        # Latitudes in the dataframe are good to go
        profile_points[:,1] = dataframe_to_filter['latitude'].values

        # Create polygon or box using lat_lim and lon_lim 
        if self.download_settings.verbose: print(f'Creating polygon...')
        if len(self.lat_lim) == 2:
            shape = [[max(self.lon_lim), min(self.lat_lim)], # Top-right
                     [max(self.lon_lim), max(self.lat_lim)], # Bottom-right
                     [min(self.lon_lim), max(self.lat_lim)], # Bottom-left
                     [min(self.lon_lim), min(self.lat_lim)]] # Top-left
        else:
            shape = []
            for lat, lon in zip(self.lat_lim, self.lon_lim):
                shape.append([lon, lat])

        # Define a t/f array for profiles within the shape
        path = mpltPath.Path(shape)
        profiles_in_range = path.contains_points(profile_points)
        
        if self.download_settings.verbose: 
            profiles_in_range_dataframe = dataframe_to_filter[profiles_in_range]
            print(f"{len(profiles_in_range_dataframe['wmoid'].unique())} floats fall within the geographic range!")   
            print(f'{len(profiles_in_range_dataframe)} profiles associated with those floats!')

        return profiles_in_range


    def __get_in_date_range(self, dataframe_to_filter: pd)-> list:
        """ A function to create and return two true false arrays indicating
            floats and profiles that fall within the geographic range.
        """
        # If the user has passed us the entire available date don't go through the whole
        # process of checking if the points of all the floats are inside the range
        beginning_of_full_range = np.datetime64(datetime(1995, 1, 1, tzinfo=timezone.utc))
        end_of_full_range = np.datetime64(datetime.now(timezone.utc))
        if self.start_date == beginning_of_full_range and self.end_date >= end_of_full_range: 
            return [True] * len(dataframe_to_filter)
        
        if self.download_settings.verbose: print(f'Sorting floats for those within the date range...')

        # Define a t/f array for dates within the range
        profiles_in_range  = ((dataframe_to_filter['date'] > self.start_date) & (dataframe_to_filter['date'] < self.end_date)).tolist()

        if self.download_settings.verbose: 
            profiles_in_range_dataframe = dataframe_to_filter[profiles_in_range]
            print(f"{len(profiles_in_range_dataframe['wmoid'].unique())} floats fall within the date range!")   
            print(f'{len(profiles_in_range_dataframe)} profiles associated with those floats!')

        return profiles_in_range


    def __get_in_time_and_space_constraints(self, dataframe_to_filter: pd)-> pd: 
        """ A function to apply the 'outside' kwarg constraints to the results after filtering by
            space and time. 
        """
        # Generate t/f arrays for profiles according to geographic and date range
        profiles_in_space = self.__get_in_geographic_range(dataframe_to_filter)
        profiles_in_time = self.__get_in_date_range(dataframe_to_filter)

        # Converting to np arrays so we can combine to make constraints
        profiles_in_space = np.array(profiles_in_space)
        profiles_in_time = np.array(profiles_in_time)

        constraints = profiles_in_time & profiles_in_space
        floats_in_time_and_space = dataframe_to_filter[constraints]
        floats_in_time_and_space = np.array(dataframe_to_filter['wmoid'].isin(floats_in_time_and_space['wmoid']))

        # Filter passed dataframe by time and space constraints to 
        # create a new dataframe to return as part of the selection frame
        if self.outside == 'time': 
            print(f'Applying outside={self.outside} constraints...')
            constraints = floats_in_time_and_space & profiles_in_space
            selection_frame = dataframe_to_filter[constraints]
       
        elif self.outside == 'space': 
            print(f'Applying outside={self.outside} constraints...')
            constraints = floats_in_time_and_space & profiles_in_time
            selection_frame = dataframe_to_filter[constraints]
        
        elif self.outside == None : 
            print(f'Applying outside=None constraints...')
            constraints = floats_in_time_and_space & profiles_in_space & profiles_in_time
            selection_frame = dataframe_to_filter[constraints]
        
        elif self.outside == 'both': 
            print(f'Applying outside={self.outside} constraints...')
            constraints = floats_in_time_and_space
            selection_frame = dataframe_to_filter[constraints]
        
        return selection_frame


    def __get_in_ocean_basin(self): 
        """ A function to drop floats/profiles outside of the specified ocean basin.
        """
        if self.download_settings.verbose: print(f"Sorting floats for those passed in 'ocean' kwarg...")
        self.selection_frame = self.selection_frame[self.selection_frame['ocean'] == str(self.ocean)]

        if self.download_settings.verbose: 
            print(f"{len(self.selection_frame['wmoid'].unique())} floats fall within the ocean basin!")
            print(f'{len(self.selection_frame)} profiles fall within the ocean basin!')


    def __dataframe_to_dictionary(self)-> dict:
        """ A function to turn the working directory into a dictionary
            of float keys with a list of profiles that match the criteria. 

            :return: narrowed_profiles : dict - A dictionary with float ID
                keys corresponding to a list of profiles that match criteria.
        """
        selected_profiles = {}
        for index, row in self.selection_frame.iterrows():
            if row['wmoid'] not in selected_profiles:
                selected_profiles[row['wmoid']] = [row['profile_index']]
            else:
                selected_profiles[row['wmoid']].append(row['profile_index'])
        return selected_profiles