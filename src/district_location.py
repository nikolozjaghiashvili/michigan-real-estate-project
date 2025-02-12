import geopandas as gpd

def parse_district_location(shapefile_path = "resources/data/school_district_map/School_District.shp"):
    df_district = gpd.read_file(shapefile_path)
    return df_district

def clean_district_location(df_district):
    df_district = df_district[['DCode','Name','Shape__Are','geometry']]
    df_district.columns = ['district_code','district_name','district_area','geometry']

    
    df_district['district_code'] = df_district['district_code'].astype('float')
    df_district['district_name'] = df_district['district_name'].astype('string')
    df_district = df_district.to_crs(epsg=4326)
    return df_district

def get_district_location(shapefile_path = "resources/data/school_district_map/School_District.shp", 
                          pkl_save = False, pkl_path = 'resources/data/district_location.pkl'):
    """
    Parameters: 
    shapefile_path (str, default= resources/data/school_district_map/School_District.shp ) - defines the dimension of bounding boxes used for querying locations; 
    pkl_save (bool, default=False) - if True, saves the processed data as a .pkl file; 
    pkl_path (str, default='resources/data/district_location.pkl') - the file path where the .pkl file will be saved if pkl_save is True.
    """
    df_district = parse_district_location(shapefile_path)
    df_district = clean_district_location(df_district)

    if pkl_save:
        df_district.to_pickle(pkl_path)
    return df_district


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('shapefile_path', help='string')
    parser.add_argument('pkl_save', help='booleon')
    parser.add_argument('pkl_path', help='string')
    args = parser.parse_args()

    df_district = get_district_location(shapefile_path = args.shapefile_path, pkl_save = args.pkl_save, pkl_path = args.pkl_path)