from Argo import Argo

# argo = Argo()
# #argo = Argo("C:/Users/steph/Dev/OneArgo-Python-Dev/oneargopy/argo_config.json")

# argo.select_profiles([160, 180])

long_lim_tests = [[160, 180], [-170, 185], [-270, 40], [90, 400], [-180, -160]]
lat_lim = [-90, 90]
longitudes = [-180, -179, -178, -177, -176, -175, -174, -173, -172, -171, -170, -169, -168, -167, -166, -165, -164, -163, -162, -161, -160, -159, -158, -157, -156, -155, -154, -153, -152, -151, -150, -149, -148, -147, -146, -145, -144, -143, -142, -141, -140, -139, -138, -137, -136, -135, -134, -133, -132, -131, -130, -129, -128, -127, -126, -125, -124, -123, -122, -121, -120, -119, -118, -117, -116, -115, -114, -113, -112, -111, -110, -109, -108, -107, -106, -105, -104, -103, -102, -101, -100, -99, -98, -97, -96, -95, -94, -93, -92, -91, -90, -89, -88, -87, -86, -85, -84, -83, -82, -81, -80, -79, -78, -77, -76, -75, -74, -73, -72, -71, -70, -69, -68, -67, -66, -65, -64, -63, -62, -61, -60, -59, -58, -57, -56, -55, -54, -53, -52, -51, -50, -49, -48, -47, -46, -45, -44, -43, -42, -41, -40, -39, -38, -37, -36, -35, -34, -33, -32, -31, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17, -16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180]
latitudes = [24.738, 43.982, -67.129, -2.345, 55.671, -89.436, -35.217, -11.632, -58.379, -20.891, -82.916, 18.524, 46.732, 32.126, 12.453, 54.879, 65.915, -45.822, 1.679, -60.931, -44.368, 35.628, 36.94, -48.014, -60.831, 34.577, -79.311, -16.521, -3.995, -43.013, -15.875, 23.435, 20.27, 75.417, -47.957, -53.35, 8.172, -65.865, 30.867, -44.155, 54.758, 79.76, -33.48, -53.115, 54.248, 7.543, 67.114, 40.503, -18.943, 46.776, 51.523, 26.007, -76.989, 33.667, 0.17, 84.669, -5.948, 79.504, 79.777, 39.786, -38.021, 55.695, -77.347, -11.438, -10.961, -20.015, -78.278, -5.172, -50.631, 10.749, 65.446, 44.458, 16.014, 31.551, 67.392, 10.882, -82.982, -47.073, 8.112, 30.003, 39.278, -41.647, 70.353, -75.023, 47.832, -53.081, 39.889, 77.42, 29.588, -22.755, -10.172, -49.181, 20.875, -47.93, 6.962, 27.17, -79.089, -15.785, -67.418, -45.801, 38.831, -79.589, -20.644, 69.028, -2.218, -46.934, -57.14, -7.213, 73.214, 54.724, -75.76, -11.075, -57.286, 52.57, 66.625, 6.89, -24.745, -77.86, 41.259, 1.66, -79.288, 75.199, 64.408, 60.969, 0.634, 65.614, -21.924, -46.993, -53.369, -4.027, 23.507, 35.822, 17.407, 43.005, -69.444, -57.455, 27.22, -50.223, -29.736, -56.501, 8.226, 36.022, -17.513, 26.255, 56.173, -69.933, -20.671, -39.868, -26.375, -20.931, -50.249, 20.875, 71.426, -66.3, 0.682, 18.234, 77.935, 27.424, -33.642, 32.763, 60.472, 19.373, -79.203, 66.206, -35.547, -49.129, -28.076, -14.938, 54.824, 45.948, 75.087, -16.282, -35.399, 23.845, 70.479, -58.224, 41.038, -48.674, 41.69, 1.173, -25.379, 61.118, -66.647, -2.428, 26.951, 73.84, 45.192, 59.267, 6.024, -29.018, -32.798, -1.017, -4.905, 79.481, -46.761, 4.689, 14.792, -44.479, 42.138, -76.911, 0.164, 82.158, 4.023, -66.574, -63.597, -57.095, -70.732, -14.769, 70.236, 68.096, -62.987, -51.884, 32.57, -63.769, 62.686, 64.25, -53.694, -36.037, 44.524, 37.095, -35.694, -64.843, -23.82, 37.262, 11.282, 31.684, 78.23, 24.513, -46.187, 3.054, -34.634, 46.049, 46.967, 52.768, 65.028, -20.228, 31.439, -61.474, 50.569, -38.537, 64.342, -18.713, 52.869, -47.334, 22.345, -54.365, -46.573, -25.949, -36.613, 14.828, 1.387, 76.377, -74.866, -22.74, 24.801, 3.743, -8.891, -69.913, 7.947, -78.855, -44.438, 53.254, 11.072, 19.676, 68.92, -40.273, -76.092, -35.734, 11.646, 32.193, -13.589, -3.738, -55.521, 6.001, -59.688, -11.108, 46.982, -12.889, 58.033, 25.69, -63.125, 77.173, -56.186, 44.388, 16.756, 34.73, -71.678, 37.163, -77.268, 41.18, -3.657, -7.866, -17.154, -50.91]

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
    print(f'Adjusted Longitudes {lon_lim}\n')

    if max(lon_lim) > 180:
            print(f'The max value in lon_lim is {max(lon_lim)}')
            print(f'Adjusting longitude values...')
            longitudes = longitudes + 360
    
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
    print(f'The geographic limits were: lon: {lon_lim} lat: {lat_lim}')
    points_saved =[]
    for i, point in enumerate(points): 
        if shape.contains(point):
            points_saved.append(point)   
    
    print(f'{len(points_saved)}/{len(points)} points were within the shape')