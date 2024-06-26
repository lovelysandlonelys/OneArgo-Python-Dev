############################################################################################################################
# Argo Functions
from Argo import Argo
import time

argo = Argo()
# argo = Argo("C:/Users/steph/Dev/OneArgo-Python-Dev/oneargopy/argo_config.json")

argo.trajectories('5905105')
argo.trajectories(5904859)
floats = [5905105, 5904859]
argo.trajectories(floats)

profiles = argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01')
argo.trajectories(list(profiles.keys()))

profiles = argo.select_profiles([100, 140], [30, 45])
argo.trajectories(list(profiles.keys()))

profiles = argo.select_profiles([0, 40])
argo.trajectories(list(profiles.keys()))

#Note: do we want to labe the points with profile nubmers?
# if we have more floats than colors we stop plotting, so fix that, maybe use a colormap instead of the fixed colors
# title super long if we have a million floats

##############################################################################################################################

# # No Criteria Specified
# start_time = time.time()
# argo.select_profiles()
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# # Testing get by ocean basin: 
# start_time = time.time()
# argo.select_profiles(start_date='2012-01-01', end_date='2012-01-02', ocean='A')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# # Testing get by float id: 
# start_time = time.time()
# argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01', floats=[5903611, 5903802, 5903807])
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# start_time = time.time()
# argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01', floats=5903611)
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# start_time = time.time()
# argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01', floats='5903611')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# # Testing outside functionality
# start_time = time.time()
# print(f'OUTSIDE = NONE')
# argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# start_time = time.time()
# print(f'OUTSIDE = TIME')
# argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01', outside='time', type='phys')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# start_time = time.time()
# print(f'OUTSIDE = SPACE')
# argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01', outside='space')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# start_time = time.time()
# print(f'OUTSIDE = BOTH')
# argo.select_profiles([-170, -168], [20, 25], '2012-01-01', '2013-01-01', outside='both')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')


# print(f'Testing Min-Max VS Rectangle:')
# print(f'Min-Max:')
# start_time = time.time()
# argo.select_profiles([-170, -168], [20, 25])
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')
# # In Argo: 
# # shape = [[max(self.lon_lim), min(self.lat_lim)], # Top-right
# #          [max(self.lon_lim), max(self.lat_lim)], # Bottom-right
# #          [min(self.lon_lim), max(self.lat_lim)], # Bottom-left
# #          [min(self.lon_lim), min(self.lat_lim)]] # Top-left

# print(f'Rectangle:')
# start_time = time.time()
# argo.select_profiles([-168, -168, -170, -170], [20, 20, 25, 25])
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')


# Same as Matlab Tests
# # Rectangular without dates
# print(f'Rectangular without dates:')
# start_time = time.time()
# argo.select_profiles([-170, -168], [20, 25])
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# # Polygon with dates
# print(f'Polygon with dates:')
# start_time = time.time()
# argo.select_profiles([38.21, 31.26, 29.77], [-74.8, -65.57, -80.16], '2013-01-01', '2020-01-01')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# # lon only with dates
# print(f'Lon only with dates:')
# start_time = time.time()
# argo.select_profiles(lon_lim=[-170, -168], start_date='2012-01-01', end_date='2014-01-01' )
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# # Lat only without dates
# print(f'Lat only without dates:')
# start_time = time.time()
# argo.select_profiles(lat_lim=[20, 25])
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')

# # Dates only
# print(f'Dates only:')
# start_time = time.time()
# argo.select_profiles(start_date='2017-01-01', end_date='2019-12-31')
# elapsed_time = time.time() - start_time
# print(f'This test took: {elapsed_time}\n')


############################################################################################################################

################################################################################################
# # Testing Longitudinal Logic

# from shapely.geometry import Point, Polygon, box
# import time

# long_lim_tests = [[-160.01723, -158.0056]]
# lat_lim = [-90, 90]

# for lon_lim in long_lim_tests:

#     # Test longitude values.
#     longitudes = [-161]
#     lon = -161
#     while lon < -158 :
#         lon += .0001
#         longitudes.append(lon)

#     latitudes = [1] * 30001
    
#     print(f'Length of lon_lim: {len(lon_lim)}')

#     if len(lon_lim) == 2:
#         print(f'lon_lim[0]: {lon_lim[0]}')
#         print(f'lon_lim[1]: {lon_lim[1]}')
#         if (lon_lim[1] <= lon_lim[0]) or (lat_lim[1] <= lat_lim[0]):
#             print(f'Longitude Limits: min={lon_lim[0]} max={lon_lim[1]}')
#             print(f'Latitude Limits: min={lat_lim[0]} max={lat_lim[1]}')
#             raise Exception(f'When passing longitude and latitude lists using the [min, max] format the max value must be greater than the min value.')
            
#     # Printing base information
#     print(f'We have {len(longitudes)} longitudes')
#     print(f'We have {len(latitudes)} latitudes')
#     print(f'Original Longitude Limits: {lon_lim}')
    
#     ## Validate Longitudes
#     ### Checking range of longitude values
#     lon_range = max(lon_lim) - min(lon_lim)
#     if lon_range > 360 or lon_range <= 0:
#         print(f'Current longitude range: {lon_range}')
#         raise Exception(f'The range between the maximum and minimum longitude values must be between 1 and 360.')
   
    
#     # Adjusting values within -180 and 
#     if  min(lon_lim) < -180:
#         print(f'Adjusting within -180')
#         lon_lim = [lon + 360.00 for lon in lon_lim]
#     print(f'Adjusted lon_lim: {lon_lim}')
    
#     # Adjusting Dataframe Values
#     adjusted_longitudes = []
#     if max(lon_lim) > 180:
#         print(f'The max value in lon_lim is {max(lon_lim)}')
#         print(f'Adjusting longitude values...')
#         for lon in longitudes:
#             if lon > -180 and lon < min(lon_lim):
#                 adjs_lon = lon + 360
#                 adjusted_longitudes.append(adjs_lon)
#             else:
#                 adjusted_longitudes.append(lon)
#     else:
#         adjusted_longitudes = longitudes 

#     points =[]
#     print(f'Creating point list from profiles.')
#     for lat, lon in zip(latitudes, adjusted_longitudes):
#         point = Point(lon, lat)
#         points.append(point)
    
#     # Create polygon or box using lat_lim and lon_lim 
#     if len(lat_lim) == 2:
#         print(f'We are making a box because the length is {len(lat_lim)}')
#         shape = box(min(lon_lim), min(lat_lim), 
#                     max(lon_lim), max(lat_lim))
#     else:
#         print(f'We are making a polygon because the length is {len(lat_lim)}')
#         coordinates = []
#         for lat, lon in zip(lat_lim, lon_lim):
#             coordinates.append([lon, lat])
#         shape = Polygon(coordinates)
#     print(f'The shape: {shape}')

#     # Create list of float ids if their corresponding coordinates
#     # are within the shape made using the lat and lon limits.
#     points_saved =[]
#     points_not_saved = []
#     for i, point in enumerate(points): 
#         if shape.contains(point):
#             points_saved.append(point)  
#         else:
#             points_not_saved.append(point) 
    
#     print(f'{len(points_saved)}/{len(points)} points were within the shape')
#     print(f'First ten points saved: {points_saved[:10]}\n')
#     print(f'Last ten points saved: {points_saved[-10:]}\n')
#     print(f'The geographic limits were: lon: {lon_lim} lat: {lat_lim}\n')
#     print(f'{len(points_saved)}/{len(points)} points were within the shape\n')
###########################################################################################################################

###########################################################################################################################
# Testing resorting dataframes
# import pandas as pd
# from Settings import DownloadSettings, SourceSettings
# from pathlib import Path
# import numpy as np

# download_settings = DownloadSettings()
# source_settings = SourceSettings()

# file_name = "argo_synthetic-profile_index.txt"
# file_path = Path.joinpath(download_settings.base_dir, 'Index', file_name)
# sprof_index  = pd.read_csv(file_path, delimiter=',', header=8, parse_dates=['date','date_update'], 
#                         date_format='%Y%m%d%H%M%S')

# # Parsing out variables in first column file
# dacs = sprof_index ['file'].str.split('/').str[0]
# sprof_index.insert(1, "dacs", dacs)

# wmoid = sprof_index ['file'].str.split('/').str[1]
# sprof_index.insert(0, "wmoid", wmoid)

# profile = sprof_index ['file'].str.split('_').str[1].str.replace('.nc', '')
# sprof_index.insert(2, "profile", profile)

# # Splitting the parameters into their own columns
# parameters_split = sprof_index ['parameters'].str.split()
# data_types_split = sprof_index ['parameter_data_mode'].apply(list)

# data_type_mapping = {np.nan: 0, 'R':1, 'A':2, 'D':3 }
# mapped_data_types_split = data_types_split.apply(lambda lst: [data_type_mapping.get(x, 0) if pd.notna(x) else 0 for x in lst])

# # Create a new DataFrame from the split lists
# expanded_df = pd.DataFrame({
#     'index': sprof_index .index.repeat(parameters_split.str.len()),
#     'parameter': parameters_split.explode(),
#     'data_type': mapped_data_types_split.explode()
# })

# # Pivot the expanded DataFrame to get parameters as columns
# with pd.option_context('future.no_silent_downcasting', True):
#     result_df = expanded_df.pivot(index='index', columns='parameter', values='data_type').fillna(0).astype('int8')

# # Fill in parameters and dacs before removing rows
# # Fill in source_settings information based off of synthetic file
# if download_settings.verbose: print(f'Filling in source settings information...')
# source_settings.set_avail_vars(sprof_index )
# source_settings.set_dacs(sprof_index )

# # Merge the pivoted DataFrame back with the original DataFrame and drop split rows
# sprof_index = sprof_index .drop(columns=['parameters', 'parameter_data_mode'])
# sprof_index = sprof_index .join(result_df)

# # Add profile_index column which counts the number of times that the same float_id is attached to 
# # a new profile file. 
# sprof_index.sort_values(by=['wmoid', 'date'], inplace=True)
# sprof_index.insert(0, "profile_index", 0)
# sprof_index['profile_index'] = sprof_index.groupby('wmoid')['date'].cumcount() + 1

# sprof_index.to_csv('output.csv', index=False)

# print(sprof_index)
###########################################################################################################################

###########################################################################################################################
# Saving this here for refrence: 

# # Create a dictionary of floats and their profiles that fall inside of the polygon
# #drop rows from working frame rather than adding to a dictionary 
# if self.download_settings.verbose: print(f'Sorting floats for those inside of the polygon...')
# profiles_in_geographic_range = {}
# for i, point in enumerate(profile_points): 
#     if shape.contains(point):
#         if (self.download_settings.float_type == 'all') and (hasattr(self.selection_frame, 'is_bgc')):
#             if self.selection_frame.iloc[i]['is_bgc']:
#                 # If we are are dealing with both frames ('all' setting) and we are on the prof frame (has 'is_bgc')
#                 # then if the 'is_bgc' value for this row is true we don't want to add this point to our dict
#                 # because it will have already been added from the sprof file. So we are going to skip the remaining code 
#                 # inside of the loop for the current iteration only by using a continue statement. 
#                 continue 
#         wmoid = self.selection_frame.iloc[i]['wmoid']
#         profile_index = self.selection_frame.iloc[i]['profile_index']
#         if wmoid not in profiles_in_geographic_range:
#             profiles_in_geographic_range[wmoid] = [profile_index]
#         else:
#             profiles_in_geographic_range[wmoid].append(profile_index)

# if self.download_settings.verbose: print(f'{len(profiles_in_geographic_range)} floats have profiles within the shape!')

# return profiles_in_geographic_range 

# Make a list of profies outside of the polygon
# remove_indexes = []
# if self.download_settings.verbose: print(f'Sorting floats for those inside of the polygon...')
# for i, point in enumerate(profile_points): 
#     if not shape.contains(point):
#         remove_indexes.extend([self.selection_frame.iloc[i].name])
        
###########################################################################################################################

###########################################################################################################################
