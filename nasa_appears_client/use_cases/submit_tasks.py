import sys
import os
from typing import List, Tuple
import geopandas as gpd
import os
from shapely.geometry import Point, Polygon
import glob
import json

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from nasa_appears_client.nasa_api import NasaApiConnection



def generate_polygon(lat:float,
                     lon:float,
                     size:Tuple[int,int]) -> gpd.GeoDataFrame:
    # Create a GeoSeries from the given coordinate
    gdf = gpd.GeoSeries([Point(lon, lat)])

    # Set the current coordinate system of the GeoSeries to WGS84 (in degrees)
    gdf.crs = 'EPSG:4326'

    # Convert the coordinate system of the GeoSeries to a UTM projection (in meters)
    gdf = gdf.to_crs('EPSG:32616')

    # Get the coordinate in UTM
    x, y = gdf.geometry[0].x, gdf.geometry[0].y

    # Define the offsets (in meters) for a 100km x 100km square
    offset_x, offset_y = size[0]/2, size[1]/2

    # Define the square Polygon
    square = Polygon([(x - offset_x, y - offset_y),
                      (x + offset_x, y - offset_y),
                      (x + offset_x, y + offset_y),
                      (x - offset_x, y + offset_y)])

    # Create a GeoDataFrame from the Polygon
    square_gdf = gpd.GeoDataFrame({'name': [1]}, geometry=[square], crs='EPSG:32616')
    # Convert the square's coordinates back to lat/long
    square_gdf = square_gdf.to_crs('EPSG:4326')
    return square_gdf


def save_shapefile_from_line(line:str,
                             size_in_meters:Tuple[int,int],
                             path:str,
                             ):
    if not os.path.exists(path):
        os.makedirs(path)

    line = line.lower()
    splitted_line = line.split(",")
    splitted_line[-1] = splitted_line[-1].replace("\n", "")
    
    # get parameters
    lat = float(splitted_line[0])
    lon = float(splitted_line[1])
    location = splitted_line[2]
    # Generate the polygon and save it
    square_gdf = generate_polygon(lat=lat,
                                  lon=lon,
                                  size=size_in_meters)
    square_gdf.to_file(os.path.join(path, f"{location}.shp"))
def generate_shapefiles_from_csv(csv_path:str,size_in_meters:Tuple[int,int],target_path:str):
    with open(csv_path, "r") as points_file:
        for line in points_file:
            save_shapefile_from_line(line=line,
                                     size_in_meters=size_in_meters,
                                     path=target_path)
            
def create_submit_requests(nasa_api,
                           size_in_meters:Tuple[int,int],
                           csv_path:str,
                           shapefiles_path:str,
                           layers:List[str],
                           start_date:str,
                           end_date:str,
                           product:str,):
    size_in_meters = [200000, 200000]
    # get the parent of the shapefiles path
    shapefiles_parent = os.path.dirname(shapefiles_path)

    generate_shapefiles_from_csv(csv_path=csv_path,
                                size_in_meters=size_in_meters,
                                target_path=shapefiles_path)
    shapefiles = glob.glob(os.path.join(shapefiles_path, "*.shp"))
    
    # create a requests folder
    if not os.path.exists(os.path.join(shapefiles_parent,"requests")):
        os.makedirs(os.path.join(shapefiles_parent,"requests"))

    
    for shapefile_path in shapefiles:
        shapefile = gpd.read_file(shapefile_path)
        name = shapefile_path.split("/")[-1].split(".")[0]
        request = nasa_api.build_submit_task_request(shapefile=shapefile,
                                                     start_date=start_date,
                                                     end_date=end_date,
                                                     product=product,
                                                     layers=layers)
        request['task_name'] = name
        request['task_type'] = 'area'
        # create a requests folder
        if not os.path.exists("requests"):
            os.makedirs("requests")
        
        with open(os.path.join(shapefiles_parent,f'requests/{name}.json'), 'w') as outfile:
            json.dump(request, outfile)
        request_id = nasa_api._submit_request(request=request)
        print(request_id)




if __name__ == "__main__":
    product = "ECO1BMAPRAD.001"
    start_date = "01-01-2018"
    end_date = "01-01-2019"
    

    # Import the shapefile
    layers = [
        "Mapped_radiance_1","Mapped_radiance_2","Mapped_radiance_3","Mapped_radiance_4","Mapped_radiance_5",
        "Mapped_data_quality_1","Mapped_data_quality_2","Mapped_data_quality_3","Mapped_data_quality_4","Mapped_data_quality_5",
        "Mapped_swir_dn","Mapped_view_azimuth","Mapped_view_zenith",   
        ]
    nasa_api = NasaApiConnection(credentials_path=os.path.join(ROOT_DIR, "credentials.json"),
                                 log_file="logs/submit_tasks.log")

    create_submit_requests(nasa_api=nasa_api,
                           csv_path=os.path.join(ROOT_DIR,"nasa_appears_client","use_cases","dataset_points.csv"),
                            shapefiles_path=os.path.join(ROOT_DIR,"nasa_appears_client","use_cases","shapefiles"),
                            size_in_meters=[200000, 200000],
                            layers=layers,
                            start_date=start_date,
                            end_date=end_date,
                            product=product)