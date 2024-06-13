############################################################################################################################
# Argo Functions
# from Argo import Argo

# argo = Argo()
# # argo = Argo("C:/Users/steph/Dev/OneArgo-Python-Dev/oneargopy/argo_config.json")

# # argo.select_profiles([-170, 185])
# argo.select_profiles([-270.7, 40.009])
############################################################################################################################

############################################################################################################################
# Testing Longitudinal Logic

from shapely.geometry import Point, Polygon, box
import time

long_lim_tests = [[-160.01723, -158.0056]]
lat_lim = [-90, 90]

for lon_lim in long_lim_tests:

    # Test longitude values.
    longitudes = [-161]
    lon = -161
    while lon < -158 :
        lon += .0001
        longitudes.append(lon)

    latitudes = [1] * 30001
    
    print(f'Length of lon_lim: {len(lon_lim)}')

    if len(lon_lim) == 2:
        print(f'lon_lim[0]: {lon_lim[0]}')
        print(f'lon_lim[1]: {lon_lim[1]}')
        if (lon_lim[1] <= lon_lim[0]) or (lat_lim[1] <= lat_lim[0]):
            print(f'Longitude Limits: min={lon_lim[0]} max={lon_lim[1]}')
            print(f'Latitude Limits: min={lat_lim[0]} max={lat_lim[1]}')
            raise Exception(f'When passing longitude and latitude lists using the [min, max] format the max value must be greater than the min value.')
            
    # Printing base information
    print(f'We have {len(longitudes)} longitudes')
    print(f'We have {len(latitudes)} latitudes')
    print(f'Original Longitude Limits: {lon_lim}')
    
    ## Validate Longitudes
    ### Checking range of longitude values
    lon_range = max(lon_lim) - min(lon_lim)
    if lon_range > 360 or lon_range <= 0:
        print(f'Current longitude range: {lon_range}')
        raise Exception(f'The range between the maximum and minimum longitude values must be between 1 and 360.')
   
    
    # Adjusting values within -180 and 
    if  min(lon_lim) < -180:
        print(f'Adjusting within -180')
        lon_lim = [lon + 360.00 for lon in lon_lim]
    print(f'Adjusted lon_lim: {lon_lim}')
    
    # Adjusting Dataframe Values
    adjusted_longitudes = []
    if max(lon_lim) > 180:
        print(f'The max value in lon_lim is {max(lon_lim)}')
        print(f'Adjusting longitude values...')
        for lon in longitudes:
            if lon > -180 and lon < min(lon_lim):
                adjs_lon = lon + 360
                adjusted_longitudes.append(adjs_lon)
            else:
                adjusted_longitudes.append(lon)
    else:
        adjusted_longitudes = longitudes 

    points =[]
    print(f'Creating point list from profiles.')
    for lat, lon in zip(latitudes, adjusted_longitudes):
        point = Point(lon, lat)
        points.append(point)
    
    # Create polygon or box using lat_lim and lon_lim 
    if len(lat_lim) == 2:
        print(f'We are making a box because the length is {len(lat_lim)}')
        shape = box(min(lon_lim), min(lat_lim), 
                    max(lon_lim), max(lat_lim))
    else:
        print(f'We are making a polygon because the length is {len(lat_lim)}')
        coordinates = []
        for lat, lon in zip(lat_lim, lon_lim):
            coordinates.append([lon, lat])
        shape = Polygon(coordinates)
    print(f'The shape: {shape}')

    # Create list of float ids if their corresponding coordinates
    # are within the shape made using the lat and lon limits.
    points_saved =[]
    points_not_saved = []
    for i, point in enumerate(points): 
        if shape.contains(point):
            points_saved.append(point)  
        else:
            points_not_saved.append(point) 
    
    print(f'{len(points_saved)}/{len(points)} points were within the shape')
    print(f'First ten points saved: {points_saved[:10]}\n')
    print(f'Last ten points saved: {points_saved[-10:]}\n')
    print(f'The geographic limits were: lon: {lon_lim} lat: {lat_lim}\n')
    print(f'{len(points_saved)}/{len(points)} points were within the shape\n')
###########################################################################################################################