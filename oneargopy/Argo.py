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
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
import shutil
import gzip
import numpy as np
from shapely.geometry import Point, Polygon, box


class Argo:
    
    def __init__(self, user_settings: str = None) -> None:
        """ The Argo constructor downloads the index files form GDAC and 
            stores them in the proper directories defined in the 
            DownloadSettings class. It then constructs dataframes
            from the argo_synthetic-profile_index.txt file and the
            ar_index_global_prof.txt file for use in class function
            calls.  

            :param: user_settings : str - An optional parameter that will be used
                to initialize the Settings classes if passed. Should be the 

            NOTE: If the user has their own settings configuration and has
            set keep_index_in_memory to false then the dataframes will be
            removed from memory at the end of construction and will be
            reloaded with following Argo function calls. 
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

        # Load the argo_synthetic-profile_index.txt file into a data frame
        if self.download_settings.verbose: print(f'\nTransferring index files into data frames...')
        self.sprof_index  = self.__load_sprof_dataframe()
        self.prof_index = self.__load_prof_dataframe()

        # Add column noting if a float is also in the sprof_index, meaning that it is a bgc float
        if self.download_settings.verbose: print(f'Marking bgc floats in prof_index...')
        self.__mark_bgcs_in_prof()
        
        # Print number of floats
        if self.download_settings.verbose: self.__display_floats() 

        if self.download_settings.verbose: print(f'Initialize is finished!\n\n')

        if not self.download_settings.keep_index_in_memory:
            if self.download_settings.verbose: print('Removing dataframes from memory...')
            del self.sprof_index 
            del self.prof_index


    def select_profiles(self, lon_lim: list = [-180, 180], lat_lim: list = [-90, 90], start_date: str = '1995-01-01', end_date: str = None, **kargs)-> dict:
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
            cycles=cycles: Select profiles by their CYCLE_NUMBER values. cycles can
                    be a scalar or an array. Only floats that have at least one
                    of the specified cycles will be returned.
            dac=dac: Select by Data Assimilation Center responsible for the floats.
                    A single DAC can be entered as a string (e.g.: 'aoml'),
                    multiple DACs can be entered as a list of strings (e.g.:
                    ['meds';'incois'].
                    Valid values as of 2024 are any: {'aoml'; 'bodc'; 'coriolis'; ...
                    'csio'; 'csiro'; 'incois'; 'jma'; 'kma'; 'kordi'; 'meds'}
            depth=depth: Select profiles that reach at least this depth
                    (positive downwards; in db)
            direction=dir: Select profiles by direction ('a' for ascending,
                    'd' for descending, '' for both directions)
            floats=floats: Select profiles only from these floats that must
                    match all other criteria
            interp_lonlat=intp : If intp is 'yes' (default), missing lon/lat
                    values (e.g., under ice) will be interpolated;
                    set intp to 'no' to suppress interpolation;
                    the default is taken from AnalysisSettings.interp_lonlat 
                    (defined during the construction of the Argo class object.)
            min_num_prof=num: Select only floats that have at least
                    num profiles that meet all other criteria
            mode=mode: Valid modes are 'R' (real-time), 'A' (adjusted), and
                    'D', in any combination. Only profiles with the selected
                    mode(s) will be listed.
                    Default is 'RAD' (all modes).
                    If multiple sensors are specified, all of them must be in
                    the selected mode(s).
                    If 'sensor' option is not used, the 'mode' option is ignored,
                    unless 'type','phys' is specified (for non-BGC floats,
                    pressure, temperature, and salinity are always in the same
                    mode).
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
            profiler',profiler: Select floats of the given profiler type (integer,
                    e.g., 846 is an APEX BGC float)
            sensor', SENSOR_TYPE: This option allows the selection by
                    sensor type. Available as of 2024: PRES, PSAL, TEMP, DOXY, BBP,
                    BBP470, BBP532, BBP700, TURBIDITY, CP, CP660, CHLA, CDOM,
                    NITRATE, BISULFIDE, PH_IN_SITU_TOTAL, DOWN_IRRADIANCE,
                    DOWN_IRRADIANCE380, DOWN_IRRADIANCE412, DOWN_IRRADIANCE443,
                    DOWN_IRRADIANCE490, DOWN_IRRADIANCE555, DOWN_IRRADIANCE670,
                    UP_RADIANCE, UP_RADIANCE412, UP_RADIANCE443, UP_RADIANCE490,
                    UP_RADIANCE555, DOWNWELLING_PAR, CNDC, DOXY2, DOXY3, BBP700_2
                    Multiple sensors can be entered as a list, e.g.: ['DOXY';'NITRATE']
            type', type: Valid choices are 'bgc' (select BGC floats only),
                    'phys' (select core and deep floats only),
                    and 'all' (select all floats that match other criteria).
                    If type is not specified, but sensors are, then the type will
                    be set to 'bgc' if sensors other than PRES, PSAL, TEMP, or CNDC
                    are specified.
                    In all other cases the default type is DownloadSettings.float_type,
                    which is set in the Argo constructor, you can also set the float_type
                    as a different value if passing a configuration file to the Argo constructor.
        """
        if self.download_settings.verbose: print(f'Starting select_profiles...')
        self.lon_lim = lon_lim
        self.lat_lim = lat_lim
        self.start_date = start_date
        self.end_date = end_date

        if self.download_settings.verbose: print(f'Validating parameters...')
        self.__validate_lon_lat_limits()
        self.__validate_start_end_dates()
        # Parse/Validate Optional Arguments Here ...

        # Load correct dataframes according to float_type
        self.__select_frame()
        
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
            self.start_date = datetime.strptime(self.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            # end_date is optional and should be set to tomorrow if not provided
            if self.end_date != None:
                self.end_date = datetime.strptime(self.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                # TESTING TESTING
                print(f'The end date with a day added is: {self.end_date}')
            else:
                self.end_date = datetime.now(timezone.utc) + timedelta(days=1)
        except ValueError:
            print(f" Start date: {self.start_date} or end date: {self.end_date} is not in the expected format 'yyyy-mm-dd'")
        
        # Validate datetimes
        if self.start_date > self.end_date:
            if self.download_settings.verbose: print(f'Current start date: {self.start_date}')
            if self.download_settings.verbose: print(f'Current end date: {self.end_date}')
            raise Exception(f'The start date must be in the past when compared to the end date.')
        if self.start_date < datetime(1995, 1, 1, tzinfo=timezone.utc):
            if self.download_settings.verbose: print(f'Current start date: {self.start_date}')
            raise Exception(f'Start date must be after at least: {datetime(1995, 1, 1, tzinfo=timezone.utc)} for any floats to be active.')
        

    def __select_frame(self):
        """ A function that determines what dataframes will be loaded/used when selecting floats. 

            If type is 'phys', the dataframe based on ar_index_global_prof.txt will be used.
            If type is 'bgc', the dataframe based on argo_synthetic-profile_index.txt will be used.
            If type is 'all', both dataframes are used, the one from argo_synthetic-profile_index.txt 
            for floats that are in it and match the criteria, the dataframe from ar_index_global_prof.txt
            for all floats that are not listed in argo_synthetic-profile_index.txt. In this 
            function we assign the selection_frame to an empty placeholder dataframe 
            because we have custom logic for the 'all' case in the __get_in_geographic_range
            function. 

            If keep_index_in_memory is set to false the dataframes created during Argo's 
            constructor are deleted. In this function we only reload the necessary
            dataframes into memory and assign them to the selection frame. 
        """
        if self.download_settings.float_type == 'phys':
            if not self.download_settings.keep_index_in_memory:
                self.prof_index = self.__load_prof_dataframe()
                self.selection_frame = self.prof_index
            else:
                self.selection_frame = self.prof_index
        elif self.download_settings.float_type == 'bgc':
            if not self.download_settings.keep_index_in_memory:
                self.sprof_index = self.__load_sprof_dataframe()
                self.selection_frame = self.sprof_index
            else:
                self.selection_frame = self.sprof_index
        elif self.download_settings.float_type == 'all':
            if not self.download_settings.keep_index_in_memory:
                self.sprof_index = self.__load_sprof_dataframe()
                self.prof_index = self.__load_prof_dataframe()
                self.selection_frame = pd.DataFrame()
            else:
                self.selection_frame = pd.DataFrame()

    
    def __narrow_profiles_by_criteria(self)-> dict:
        """ A function to narrow down the available profiles to only those
            that meet the criteria passed to select_profiles.

            :return: narrowed_profiles : dict - A dictionary with float ID
                keys corresponding to a list of profiles that match criteria.
        """
        # The case for pulling information from both dataframes
        if self.selection_frame.empty:
            self.selection_frame = self.sprof_index
            profiles_in_geographic_range = self.__get_in_geographic_range()
            self.selection_frame = self.prof_index
            profiles_in_geographic_range.update(self.__get_in_geographic_range())
        # The case for pulling information from a single dataframe
        else:
            profiles_in_geographic_range = self.__get_in_geographic_range()

        # Printing Dict, likely to remove after testing period
        print(f'Here is the dictionary after filtering:')
        for key, value in profiles_in_geographic_range.items():
            print(f'{key}: {value}')


    def __get_in_geographic_range(self) -> dict:
        """ A function to compile floats within a certain geographic range.

            :return: narrowed_profiles : dict - A dictionary with float ID
                keys corresponding to a list of profiles that match 
                latitude and longitude criteria.
        """
        # The longitudes in the dataframe are standardized to fall within -180 and 180.
        # but our longitudes only have a standard minimum value of -180. In this section
        # we adjust the longitude and latitudes in the dataframe to follow this minimum 
        # only approach.
        if max(self.lon_lim) > 180:
            if self.download_settings.verbose: print(f'The max value in lon_lim is {max(self.lon_lim)}')
            if self.download_settings.verbose: print(f'Adjusting longitude values...')
            lons = self.selection_frame['longitude'].apply(lambda x: x + 360 if -180 < x < min(self.lon_lim) else x)
        else:
            lons = self.selection_frame['longitude'] 
        
        # Latitudes in the dataframe are good to go
        lats = self.selection_frame['latitude'] 

        # Make Point objects out of profile lat and lons
        profile_points =[]
        if self.download_settings.verbose: print(f'Creating point list from profiles...')
        for lat, lon in zip(lats, lons):
            point = Point(lon, lat)
            profile_points.append(point)

        # Create polygon or box using lat_lim and lon_lim 
        if self.download_settings.verbose: print(f'Creating polygon...')
        if len(self.lat_lim) == 2:
            shape = box(min(self.lon_lim), min(self.lat_lim), 
                        max(self.lon_lim), max(self.lat_lim))
        else:
            coordinates = []
            for lat, lon in zip(self.lat_lim, self.lon_lim):
                coordinates.append([lon, lat])
            shape = Polygon(coordinates)

        # Create a dictionary of floats and their profiles that fall inside of the polygon
        if self.download_settings.verbose: print(f'Sorting floats for those inside of the polygon...')
        profiles_in_geographic_range = {}
        for i, point in enumerate(profile_points): 
            if shape.contains(point):
                if (self.download_settings.float_type == 'all') and (hasattr(self.selection_frame, 'is_bgc')):
                    if self.selection_frame.iloc[i]['is_bgc']:
                        # If we are are dealing with both frames ('all' setting) and we are on the prof frame (has 'is_bgc')
                        # then if the 'is_bgc' value for this row is true we don't want to add this point to our dict
                        # because it will have already been added from the sprof file. So we are going to skip the remaining code 
                        # inside of the loop for the current iteration only by using a continue statement. 
                        continue 
                wmoid = self.selection_frame.iloc[i]['wmoid']
                profile_index = self.selection_frame.iloc[i]['profile_index']
                if wmoid not in profiles_in_geographic_range:
                    profiles_in_geographic_range[wmoid] = [profile_index]
                else:
                    profiles_in_geographic_range[wmoid].append(profile_index)
        
        if self.download_settings.verbose: print(f'{len(profiles_in_geographic_range)} floats have profiles within the shape!')

        return profiles_in_geographic_range        


    def __get_in_date_range(self):
        pass