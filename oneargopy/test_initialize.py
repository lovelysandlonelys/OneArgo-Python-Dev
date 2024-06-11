from Argo import Argo
from shapely.geometry import Point, Polygon, box


# argo = Argo()
# #argo = Argo("C:/Users/steph/Dev/OneArgo-Python-Dev/oneargopy/argo_config.json")

# argo.select_profiles([160, 180])

long_lim_tests = [[160, 180], [-170, 185], [-270, 40], [90, 400], [-180, -160]]
lat_lim = [-90, 90]
longitudes = [-180, -179, -178, -177, -176, -175, -174, -173, -172, -171, -170, -169, -168, -167, -166, -165, -164, -163, -162, -161, -160, -159, -158, -157, -156, -155, -154, -153, -152, -151, -150, -149, -148, -147, -146, -145, -144, -143, -142, -141, -140, -139, -138, -137, -136, -135, -134, -133, -132, -131, -130, -129, -128, -127, -126, -125, -124, -123, -122, -121, -120, -119, -118, -117, -116, -115, -114, -113, -112, -111, -110, -109, -108, -107, -106, -105, -104, -103, -102, -101, -100, -99, -98, -97, -96, -95, -94, -93, -92, -91, -90, -89, -88, -87, -86, -85, -84, -83, -82, -81, -80, -79, -78, -77, -76, -75, -74, -73, -72, -71, -70, -69, -68, -67, -66, -65, -64, -63, -62, -61, -60, -59, -58, -57, -56, -55, -54, -53, -52, -51, -50, -49, -48, -47, -46, -45, -44, -43, -42, -41, -40, -39, -38, -37, -36, -35, -34, -33, -32, -31, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17, -16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180]
latitudes = my_list = [1] * 361
print(f'We have {len(longitudes)} longitudes')
print(f'We have {len(latitudes)} latitudes')

for lon_lim in long_lim_tests:
    print(f'Original Longitudes {lon_lim}')
    ## Validate Longitudes
    ### Checking range of longitude values
    lon_range = max(lon_lim) - min(lon_lim)
    if lon_range > 360 or lon_range <= 0:
        print(f'Current longitude range: {lon_range}')
        raise Exception(f'The range between the maximum and minimum longitude values must be between 1 and 360.')
    ### Adjusting values so that minimum longitude is never smaller than -180
    while min(lon_lim) < -180:
        lon_lim = [lon + 360 for lon in lon_lim]
    print(f'Adjusted Longitudes {lon_lim}')

    if max(lon_lim) > 180:
            print(f'The max value in lon_lim is {max(lon_lim)}')
            print(f'Adjusting longitude values...')
            longitudes = [lon + 360 for lon in longitudes]
    
    # Make points out of profile lat and lons
    points =[]
    print(f'Creating point list from profiles.')
    for lat, lon in zip(latitudes, longitudes):
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
    for i, point in enumerate(points): 
        if shape.contains(point):
            points_saved.append(point)   
    
    print(f'{len(points_saved)}/{len(points)} points were within the shape')
    print(f'The geographic limits were: lon: {lon_lim} lat: {lat_lim}')
    print(f'The points: {points_saved}\n')