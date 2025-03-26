#!/usr/bin/env python

"""
Fractional cover zonal statistics pipeline
==========================================

Description: This pipeline comprises of 12 scripts which read in the Rangeland Monitoring Branch odk instances
{instance names, odk_output.csv and ras_odk_output.csv: format, .csv: location, directory}
Outputs are files to a temporary directory located in your working directory (deleted at script completion),
or to an export directory located a the location specified by command argument (--export_dir).
Final outputs are files to their respective property sub-directories within the Pastoral_Districts directory located in
the Rangeland Working Directory.


step1_1_initiate_fractional_cover_zonal_stats_pipeline.py
===============================
Description: This script initiates the Fractional cover zonal statistics pipeline.
This script:

1. Imports and passes the command line arguments.

2. Creates two directories named: user_YYYYMMDD_HHMM. If either of the directories exist, they WILL BE DELETED.

3. Controls the workflow of the pipeline.

4. deletes the temporary directory and its contents once the pipeline has completed.


Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
Version: 1.0

###############################################################################################

MIT License

Copyright (c) 2020 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##################################################################################################

===================================================================================================

Command arguments:
------------------

--tile_grid: str
string object containing the path to the Landsat tile grid shapefile.

--directory_odk: str
string object containing the path to the directory housing the odk files to be processed - the directory can contain 1
to infinity odk outputs.
Note: output runtime is approximately 1 hour using the remote desktop or approximately  3 hours using your laptop
(800 FractionalCover images).

--export_dir: str
string object containing the location of the destination output folder and contents(i.e. 'C:Desktop/odk/YEAR')
NOTE1: the script will search for a duplicate folder and delete it if found.
NOTE2: the folder created is titled (YYYYMMDD_TIME) to avoid the accidental deletion of data due to poor naming
conventions.

--image_count
integer object that contains the minimum number of Landsat images (per tile) required for the fractional cover
zonal stats -- default value set to 800.

--landsat_dir: str
string object containing the path to the Landsat Directory -- default value set to r'Z:\Landsat\wrs2'.

--no_data: int
ineger object containing the Landsat Fractional Cover no data value -- default set to 0.

--rainfall_dir: str
string object containing the pathway to the rainfall image directory -- default set to r'Z:\Scratch\mcintyred\Rainfall'.

--search_criteria1: str
string object containing the end part of the filename search criteria for the Fractional Cover Landsat images.
-- default set to 'dilm2_zstdmask.img'

--search_criteria2: str
string object from the concatenation of the end part of the filename search criteria for the Fractional Cover
Landsat images. -- default set to 'dilm3_zstdmask.img'

--search_criteria3: str
string object from the concatenation of the end part of the filename search criteria for the QLD Rainfall images.
-- default set to '.img'

======================================================================================================

"""

# Import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings
import glob
import pandas as pd
import geopandas

warnings.filterwarnings("ignore")


def get_cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')

    p.add_argument('-d', '--data', help='The directory the site points csv file.')

    p.add_argument('-x', '--export_dir',
                   help='Enter the export directory for all of the final outputs.',
                   default=r'C:\Users\robot\projects\outputs\fire_zonal')

    p.add_argument('-i', '--image_count', type=int,
                   help='Enter the minimum amount of Landsat images required per tile as an integer (i.e. 950).',
                   default=100)

    p.add_argument('-fa', '--fire_analysis', help="The path to the fire analysis directory",
                   default=r"H:\fire_footprint\fire_analysis_tile")

    p.add_argument('-n', '--no_data', help="Enter the Fire no data value (i.e. -1)",
                   default=-1)

    p.add_argument('-t', '--tile', help="Landsat tile (106_069)")

    p.add_argument('-z', '--zone', help="Landsat tile zone (i.e. 2, 3 or 4)")


    cmd_args = p.parse_args()

    if cmd_args.data is None:
        p.print_help()

        sys.exit()

    return cmd_args


def temporary_dir_fn():
    """ Create a temporary directory 'user_YYYMMDD_HHMM'.

    @return temp_dir_path: string object containing the newly created directory path.
    @return final_user: string object containing the user id or the operator.
    """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    final_user = user  #[3:]

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    temp_dir_path = '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(temp_dir_path)

    except:
        print('The following temporary directory will be created: ', temp_dir_path)
        pass
    # create folder a temporary folder titled (titled 'tempFolder'
    os.makedirs(temp_dir_path)

    return temp_dir_path, final_user


def temp_dir_folders_fn(temp_dir_path):
    """ Create folders within the temp_dir directory.

    @param temp_dir_path: string object containing the newly created directory path.
    @return prime_temp_grid_dir: string object containing the newly created folder (temp_tile_grid) within the
    temporary directory.
    @return prime_temp_buffer_dir: string object containing the newly created folder (temp_1ha_buffer)within the
    temporary directory.

    """

    prime_temp_grid_dir = temp_dir_path + '\\temp_tile_grid'
    os.mkdir(prime_temp_grid_dir)

    zonal_stats_ready_dir = prime_temp_grid_dir + '\\zonal_stats_ready'
    os.makedirs(zonal_stats_ready_dir)

    proj_tile_grid_sep_dir = prime_temp_grid_dir + '\\separation'
    os.makedirs(proj_tile_grid_sep_dir)

    prime_temp_buffer_dir = temp_dir_path + '\\temp_1ha_buffer'
    os.mkdir(prime_temp_buffer_dir)

    gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    os.mkdir(gcs_wgs84_dir)

    albers_dir = (temp_dir_path + '\\albers')
    os.mkdir(albers_dir)

    return prime_temp_grid_dir, prime_temp_buffer_dir


def export_file_path_fn(export_dir, final_user, tile):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument export_dir.

    @param final_user: string object containing the user id or the operator.
    @param export_dir: string object containing the path to the export directory (command argument).
    @return export_dir_path: string object containing the newly created directory path for all retained exports.
    """

    # create string object from final_user and datetime.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    export_dir_path = export_dir + '\\' + final_user + '_' + str(tile) + '_fire_nafi_zonal_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        print('The following export directory will be created: ', export_dir_path)
        pass

    # create folder.
    os.makedirs(export_dir_path)

    return export_dir_path


def export_dir_folders_list_fn(export_dir_path, sub_dir_list):
    """ Create sub-folders within the export directory.

    @param export_dir_path: string object containing the newly created export directory path.
    @return tile_status_dir: string object containing the newly created folder (tile_status) with three sub-folders:
    for_processing, insufficient_files and tile_status_lists.
    @return tile_status_dir:
    @return plot_dir:
    @return zonal_stats_output_dir:
    @return rainfall_output_dir:
    """

    print("export_dir_folders_list_fn init..")
    print("sub_dir_list: ", sub_dir_list)

    #os.mkdir(export_dir_path)
    ex_dir_path_list = []
    for i in sub_dir_list:
        i_output_dir = (os.path.join(export_dir_path, i))
        ex_dir_path_list.append(i_output_dir)

    return ex_dir_path_list


def export_dir_folders_fn(out):
    """ Create sub-folders within the export directory.

    @param export_dir_path: string object containing the newly created export directory path.
    @return tile_status_dir: string object containing the newly created folder (tile_status) with three sub-folders:
    for_processing, insufficient_files and tile_status_lists.
    @return tile_status_dir:
    @return plot_dir:
    @return zonal_stats_output_dir:
    @return rainfall_output_dir:
    """
    ex_dir_path_list = []
    for i in out:
        os.mkdir(i)
        print(f"Created: {i}")
        ex_dir_path_list.append(i)


def find_directories_with_file_type(root_dir, file_extension):
    #print("root_dir: ", root_dir)
    # List to store directories containing the specified file type
    directories = set()

    # Walk through the directory tree
    for dirpath, dirnames, filenames in os.walk(root_dir):
        #print("dirpath: ", dirpath)
        #print(filenames)
        # Check for files with the specified extension
        for filename in filenames:
            if filename.endswith(file_extension):
                directories.add(dirpath)
                break  # No need to check more files in this directory

    return directories


def split_path_at_3th_dir(path):
    # Split the path into its components
    parts = path.split(os.sep)
    #print("parts: ", parts)
    # import sys
    # sys.exit()

    # Check if the path has at least 3 directories
    if len(parts) < 4:  # 3 directories + 1 for the root directory
        raise ValueError(f"Path does not have at least 3 directories: {parts}")

    # Join the parts back into two paths
    first_part = os.sep.join(parts[:3])
    second_part = os.sep.join(parts[3:])

    return first_part, second_part


import geopandas as gpd
from pyproj import CRS


def reproject_shapefile(input_path, zone, output_directory):
    """
    Reprojects a shapefile based on the specified UTM zone and saves it to a new directory.

    Parameters:
    - input_path: str, path to the input shapefile.
    - zone: int, UTM zone (2, 3, or 4).
    - output_directory: str, path to the directory where the reprojected shapefile will be saved.

    Returns:
    - output_path: str, path to the reprojected shapefile.
    """

    # Define the EPSG codes for WGS 84 UTM zones 52, 53, and 54
    epsg_dict = {2: 32652, 3: 32653, 4: 32654}

    if zone not in epsg_dict:
        raise ValueError("Zone must be 2, 3, or 4.")

    # Read the shapefile
    gdf = gpd.read_file(input_path)

    # Reproject to the specified zone
    target_crs = CRS.from_epsg(epsg_dict[zone])
    gdf_reprojected = gdf.to_crs(target_crs)

    # Define the output path
    output_filename = f"{output_directory}/reprojected_zone_{zone}.shp"

    # Save the reprojected shapefile
    gdf_reprojected.to_file(output_filename)

    return output_filename

def main_routine():
    """ Description: This script determines which Landsat tile had the most non-null zonal statistics records per site
    and files those plots (bare ground, all bands and interactive) into final output folders. """

    # print('fcZonalStatsPipeline.py INITIATED.')
    # read in the command arguments
    cmd_args = get_cmd_args_fn()
    data = cmd_args.data
    export_dir = cmd_args.export_dir
    fire_analysis = cmd_args.fire_analysis
    #season = cmd_args.season
    no_data = int(cmd_args.no_data)
    image_count = int(cmd_args.image_count)
    tile = cmd_args.tile
    zone = cmd_args.zone

    tile_grid = r"C:\Users\robot\code\pipelines\tile_fire_biomass_zonal_pipeline\assets\Landsat_wrs2_TileGrid.shp"



    fire_dict = {f"{tile}_ann_gap_aavgf":[-1.0],
                 f"{tile}_ann_fsm_afsum":[-1.0],
                 f"{tile}_ann_rio_afrio": [-1.0],
                 f"{tile}_ann_fyn_afysn": [-1.0],
                 f"{tile}_ann_pos_aposf": [-1.0],

                 f"{tile}_dry_gap_davgf": [-1.0],
                 f"{tile}_dry_fsm_dfsum": [-1.0],
                 f"{tile}_dry_rio_dfrio": [-1.0],
                 f"{tile}_dry_fyn_dfysn": [-1.0],
                 f"{tile}_dry_pos_dposf": [-1.0],

                 f"{tile}_lds_gap_lavgf": [-1.0],
                 f"{tile}_lds_fsm_lfsum": [-1.0],
                 f"{tile}_lds_rio_lfrio": [-1.0],
                 f"{tile}_lds_fyn_lfysn": [-1.0],
                 f"{tile}_lds_pos_lposf": [-1.0],
                 }

    # call the temporaryDir function.
    temp_dir_path, final_user = temporary_dir_fn()
    # call the tempDirFolders function.
    prime_temp_grid_dir, prime_temp_buffer_dir = temp_dir_folders_fn(temp_dir_path)
    print("prime_temp_grid_dir: ", prime_temp_grid_dir)
    # call the exportFilepath function.
    export_dir_path = export_file_path_fn(export_dir, final_user, tile)

    tile_path = os.path.join(fire_analysis, tile)
    print("tile_path: ", tile_path)



    # Begin finding directory paths
    root_directory = tile_path
    extension = '.tif'  # Change this to the file extension you're looking for
    directories = find_directories_with_file_type(root_directory, extension)

    # Create a list of all directories that contain tiff files
    list_of_directories = []
    list_of_dir_create = []
    for directory in directories:
        list_of_directories.append(directory)
        print("-" * 20)
        print(directory)

        first_part, second_part = split_path_at_3th_dir(directory)


        print("First part:", first_part)
        print("Second part:", second_part)
        out_sub_dir = second_part.replace("\\", "_")
        print("out_sub_dir: ", out_sub_dir)
        list_of_dir_create.append(out_sub_dir)

    # import sys
    # sys.exit()
    print("list_of_dir_create: ", list_of_dir_create)
    print("export_dir_path: ", export_dir_path)
    ex_dir_path_list = export_dir_folders_list_fn(export_dir_path, list_of_dir_create)
    print("ex_dir_path_list: ", ex_dir_path_list)
    # import sys
    # sys.exit()

    #prop_of_interest = "None"


    sub_dir_list_csv = []

    select_o = []
    select_i = []
    select_c = []
    select_d = []
    datesplit_s = []
    datesplit_e = []

    create_ex_dir = []

    # Define a tuple of suffixes
    suffixes = ("afrio", "aposf", "afsum", "afysn", "aavgf",
                "dfrio", "dposf", "dfsum", "dfysn", "davgf",
                "lfrio", "lposf", "lfsum", "lfysn", "lavgf",)


    for i, o, d in zip(list_of_directories, ex_dir_path_list, list_of_dir_create):
        print("i: ", i)
        print("o:", o)
        print("data_type: ", d)


        if i.endswith("no0"):
            print("i cor: ", i)
            print("o cor:", o)
            select_o.append(o)
            select_i.append(i)
            select_d.append(d)
            # datesplit_s.append(-19)
            # datesplit_e.append(-11)
            datesplit_s.append(-22)
            datesplit_e.append(-16)
            import step1_2_list_of_qld_grid_images
            export_csv = step1_2_list_of_qld_grid_images.main_routine(i, o, d, "tif", temp_dir_path)
            #export_dir_path, type_dir, ".tif", str(i), sub_dir_list, fire_analysis, fire_ver)
            select_c.append(export_csv)

        #elif i.endswith("afrio") or i.endswith("aposf") or i.endswith("afsum") or i.endswith("adfys") or i.endswith("afysn"):
        elif any(i.endswith(suffix) for suffix in suffixes):
            #ratio, possion, sum of fires and distance and fire yn
            print("i siav or simd: ", i)
            print("o siav or simd:", o)
            select_o.append(o)
            select_i.append(i)
            select_d.append(d)
            datesplit_s.append(-23)
            datesplit_e.append(-17)
            import step1_2_list_of_qld_grid_images
            export_csv = step1_2_list_of_qld_grid_images.main_routine(i, o, d, "tif", temp_dir_path)
            #export_dir_path, type_dir, ".tif", str(i), sub_dir_list, fire_analysis, fire_ver)
            select_c.append(export_csv)
        #todo change mmed and mavg to smed and savg
        # elif i.endswith("mavg") or i.endswith("mmed"):
        #     print("i mavg or mmed: ", i)
        #     print("o mavg or mmed:", o)
        #     select_o.append(o)
        #     select_i.append(i)
        #     select_d.append(d)
        #     datesplit_s.append(-23)
        #     datesplit_e.append(-17)
        #     import step1_2_list_of_qld_grid_images
        #     export_csv = step1_2_list_of_qld_grid_images.main_routine(i, o, d, "tif", temp_dir_path)
        #     #export_dir_path, type_dir, ".tif", str(i), sub_dir_list, fire_analysis, fire_ver)
        #     select_c.append(export_csv)
        else:
            pass

    print(export_csv)
    export_dir_folders_fn(select_o)

    # import sys
    # sys.exit()
    # import step1_3_project_buffer
    # geo_df2, crs_name = step1_3_project_buffer.main_routine(data, export_dir_path, prime_temp_buffer_dir)
    #
    # geo_df2.reset_index(drop=True, inplace=True)
    # geo_df2['uid'] = geo_df2.index + 1
    # #geo_df2.to_file(os.path.join(export_dir_path, "biomass_1ha.shp"))
    #
    # shapefile_path = os.path.join(export_dir_path, "biomass_1ha_all_sites.shp")
    # geo_df2.to_file(os.path.join(shapefile_path),
    #                 driver="ESRI Shapefile")
    #
    # print("Exported shapefile: ", shapefile_path)

    # -------------------------------------------

    #print("data: ", data)
    import step1_3_project_buffer_tile
    geo_df2, crs_name = step1_3_project_buffer_tile.main_routine(data, zone, export_dir_path, prime_temp_buffer_dir)


    import step1_4_landsat_tile_grid_identify2
    comp_geo_df, zonal_stats_ready_dir = step1_4_landsat_tile_grid_identify2.main_routine(
        tile_grid, geo_df2, data, zone, export_dir_path, prime_temp_grid_dir)

    print("zonal_stats_ready_dir: ", zonal_stats_ready_dir)
    comp_geo_df.to_file(os.path.join(export_dir_path, "biomass_1ha.shp"))
    print("comp_geo_df: ", os.path.join(export_dir_path, "biomass_1ha.shp"))
    print("comp_geo_df: ", comp_geo_df)



    lsat_tile = f"{tile[:3]}{tile[4:]}"
    #tile = str(path) + str(row)
    print("tile: ", tile)
    geo_df3 = comp_geo_df[comp_geo_df["tile"] == lsat_tile]
    print("geo_df3: ", geo_df3)
    geo_df4 = geo_df3[["site_name", "tile", "geometry"]]
    print("geodf4: ", geo_df4)


    geo_df4.reset_index(drop=True, inplace=True)
    geo_df4['uid'] = geo_df4.index + 1

    shapefile_path = os.path.join(export_dir_path, "biomass_1ha_{0}.shp".format(tile))
    geo_df4.to_file(os.path.join(export_dir_path, "biomass_1ha_{0}.shp".format(tile)),
                    driver="ESRI Shapefile")

    print("Exported shapefile: ", shapefile_path)


    # import sys
    # sys.exit()

    for in_dir, out_dir, csv_list, data_type, date_s, date_e in zip(select_i, select_o, select_c,
                                                                    select_d, datesplit_s, datesplit_e):
        print("data_type : ", data_type)

        # import sys
        # sys.exit()
        if data_type.endswith("no0"):
            print("data type: ", data_type)
            import step1_8_qld_grid_zonal_stats_class
            step1_8_qld_grid_zonal_stats_class.main_routine(
                in_dir, out_dir, csv_list, data_type, temp_dir_path, geo_df2, shapefile_path, date_s,
                date_e, fire_dict)

        else:
            print("standard")
            import step1_8_qld_grid_zonal_stats
            step1_8_qld_grid_zonal_stats.main_routine(
                in_dir, out_dir, csv_list, data_type, temp_dir_path, geo_df2, shapefile_path, date_s,
                date_e, fire_dict)

        print(f"completed: ", out_dir)

    # ---------------------------------------------------- Clean up ----------------------------------------------------

    shutil.rmtree(temp_dir_path)
    print('Temporary directory and its contents has been deleted from your working drive.')
    print(' - ', temp_dir_path)
    print('met zonal stats pipeline is complete.')
    print('goodbye.')


if __name__ == '__main__':
    main_routine()
