Active development moved to https://code.usgs.gov/wma/wp/2wp-temp-observations

# Purpose
This repository contains a pipeline to assemble water temperature data for streams across the United States.

# Contact
Contact Samantha Oliver (soliver@usgs.gov) for more information about the code or data contained in this pipeline.

# How it works
This pipeline is orchestrated by the R package `scipiper`. The steps of the pipeline
are ordered by number prefixes and represent major steps in the retrieval or processing pipeline.
Steps and dependencies between steps are orchestrated via yaml files that contain the "recipes"
for each target. Functions and the core code that executes the work are in `src` folders. 
Other miscellaneous files are used to track whether files and targets are up-to-date 
(e.g., `.ind` files and all files in `build/status`) relative to the shared data storage location. 

`1_` files retrieve and process Water Quality Portal (WQP) data.

`2_` files retrieve and process National Water Information System (NWIS) data.

`4_` files retrieve and process other sources of water temperature data, including EcoSHEDS and NorWeST temperature databases.

`5_` files bring all data sources together, reduce redundant data, and performs outlier detection.

`6_` files match the temperature monitoring sites to the National Geospatial Fabric stream network. 
