import requests
import json
import urllib.parse

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon


def get_bounding_boxes(box_dimension):
    
    xmin, ymin, xmax, ymax = -10067827.77, 5106107.12, -9141446.38, 6160601.91
    rows, cols = box_dimension, box_dimension
    x_step = (xmax - xmin) / cols
    y_step = (ymax - ymin) / rows
    subboxes = {}
    count = 1
    for i in range(rows):
        for j in range(cols):
            subboxes[f"box_{count}"] = {
                "xmin": xmin + (j * x_step),
                "ymin": ymin + (i * y_step),
                "xmax": xmin + ((j + 1) * x_step),
                "ymax": ymin + ((i + 1) * y_step),
            }
            count += 1
    
    base_url = "https://gisp.mcgi.state.mi.us/arcgis/rest/services/MDE/CEPIDashboardMap/MapServer/0/query"   
    common_params = """f=json&maxRecordCountFactor=3&outFields=%2A&outSR=102100&resultType=tile&returnExceededLimitFeatures=false&spatialRel=esriSpatialRelIntersects&where=1%3D1%20AND%20ENTTYPENM%20NOT%20IN%20%28%27Nonpublic%20School%27%2C%20%27Administrative%20Unit%27%2C%20%27HIGHER_ED%27%2C%20%27ISD%20Non-Instructional%20Ancillary%20Facility%27%2C%20%27LEA%20Non-Instructional%20Ancillary%20Facility%27%2C%20%27State%20Non-Instructional%20Ancillary%20Facility%27%29%20AND%20STATUS%20NOT%20LIKE%20%27%25Closed%25%27&geometryType=esriGeometryEnvelope&inSR=102100
"""
    
    url_list = []
    for key, box in subboxes.items():
        geometry = {
            "spatialReference": {"latestWkid": 3857, "wkid": 102100},
            "xmin": box["xmin"],
            "ymin": box["ymin"],
            "xmax": box["xmax"],
            "ymax": box["ymax"],
        }
    
        geometry_param = urllib.parse.quote(json.dumps(geometry))
        url = f"{base_url}?geometry={geometry_param}&{common_params}"
        url_list.append(url)
    return url_list


def parse_school_location(url_list):
    results = []
    for url in url_list:
        response = requests.get(url)
        dict_list = [i["attributes"] for i in response.json().get("features", [])]
        print(f"{len(dict_list)} Schools in the list")
        results.extend(dict_list)
    df_school = pd.DataFrame(results)
    return df_school


def clean_school_location(df_school):
    df_school = df_school[['ENTITYCD','OFFICIALNM','LATITUDE','LONGITUDE']]
    df_school.columns = ['school_code','school_name','latitude','longitude']
    df_school = gpd.GeoDataFrame(df_school, geometry=gpd.points_from_xy(df_school.latitude, df_school.longitude), crs="EPSG:4326")
    return df_school


def get_school_location(box_dimension = 8, pkl_save = False, pkl_path = 'resources/data/school_location.pkl'):
    """
    Parameters: 
    box_dimension (int, default=8) - defines the dimension of bounding boxes used for querying locations; 
    pkl_save (bool, default=False) - if True, saves the processed data as a .pkl file; 
    pkl_path (str, default='resources/data/school_location.pkl') - the file path where the .pkl file will be saved if pkl_save is True.
    """
    url_list = get_bounding_boxes(8)
    df_school = parse_school_location(url_list)
    df_school = clean_school_location(df_school)

    if pkl_save:
        df_school.to_pickle(pkl_path)
    return df_school


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('box_dimension', help='integer')
    parser.add_argument('pkl_save', help='booleon')
    parser.add_argument('pkl_path', help='string')
    args = parser.parse_args()

    df_school = get_school_location(box_dimension = args.box_dimension, pkl_save = args.pkl_save, pkl_path = args.pkl_path)
    


