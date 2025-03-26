import os
import subprocess

# Specify the parent directory
parent_dir = r"H:\fire_footprint\fire_analysis_tile2"

# Get a list of directories in the parent directory
dir_list = [d for d in os.listdir(parent_dir) if os.path.isdir(os.path.join(parent_dir, d))]

# Path to the Python script to be executed
script_path = r"C:\Users\robot\code\pipelines\tile_fire_biomass_zonal_pipeline\code\step1_1_initiate_fractional_cover_zonal_stats_pipeline.py"

# Loop through the list of directories
for directory in dir_list:
    # Construct the full path
    full_path = os.path.join(parent_dir, directory)
    print(f"Processing directory: {full_path}")
    print(directory)

    # # Command-line arguments for the script
    # arguments = [
    #     r"C:\Users\robot\anaconda3\envs\biomass_zonal\python.exe",  # Python interpreter
    #     script_path,  # Path to the script
    #     "--tile", directory,  # Directory argument
    #     "--zone", "2",  # Example argument value
    #     "--data", r"C:\Users\robot\projects\biomass\collated_agb\20241221\slats_tern_biomass.csv"  # CSV file path
    # ]
    #
    # # Run the subprocess
    # try:
    #     print(f"Running script for directory: {full_path}")
    #     subprocess.run(arguments, check=True)
    #     print(f"Completed processing for: {full_path}")
    # except subprocess.CalledProcessError as e:
    #     print(f"Error running script for {full_path}: {e}")



