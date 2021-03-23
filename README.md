# Alma-Migration-Scripts

Several scripts to clean up and enhance the data after migrating from Aleph to Alma

## Requirements

### Python version and external libraries

All scripts have been written using python 3.7. The scripts make us of several common packages. These need to be provided: 
* numpy
* pandas
* requests
* requests_cache
* lxml
* xlrd (for reading Excel tables)

### Additional requirements

In addition a valid API key for the alma institution needs to be provided.
to obtain a key log into ExLibris Developer Network (https://developers.exlibrisgroup.com/login/), go to "Build" and "My API Keys".
If you are not allowed to generate keys, please contact your local group administrator.  
The API key is to be present as environment variable "ALMA_SCRIPT_API_KEY"

## Folder structure:

The scripts make use of a folder structure to organize input, temporary and output files.
In particular.

* Input files are searched for in a folder data/input relative to the script file.
* Temporary date are stored in a folder data/temp relative to the script file
* Output files are stored in a folder data/output relative to the script file

## Script structure

The principal setup is done at the end iof the file in the main section. 
In general a project name is defined, which defines the names of the input files (see the descritpion of each script for details).

If a list is to be loaded by the list_reader_service (load_identifier_list_of_type) the list name hase the format <list_type>_list.txt.
e.g. a list of ill partners of list type "partners" would read partners_list.txt.

## Individual Scripts

### marc_processor.py

Files to be processed are to be placed in the /data/input folder. Filter chains are defined in the /chains folder as json documents.

Each chain is named accoding to "filter_chain_<project>.json". The projects to be run are defined in the main function of the marc_processor.py:

projects = ['ebooks_lizenzfrei', 'collections_from_db', 'zsn_ezb']. This would load the chain files

* "filter_chain_ebooks_lizenzfrei.json"
* "filter_chain_collections_from_db.json"
* "filter_chain_zsn_ezb.json"

and run the corresponding filter chain. 
Logging is provided in the corresponding log files ("marc_processor_<project>.log") in the output folder (/output) whereas the generated files are found in a subfolder of the output folder named according to the project

### clean_up_after_migration.py

### clean_up_list.py

### XML_collector.py

