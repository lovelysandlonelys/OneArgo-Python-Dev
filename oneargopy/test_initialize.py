#from Argo import Argo

#argo = Argo()
#argo = Argo("C:/Users/steph/Dev/OneArgo-Python-Dev/oneargopy/argo_config.json")

#argo.select_profiles()

long_lim_tests = [[160, 180], [-170, 185], [-270, 40], [90, 400], [-120, -130, -125]]

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