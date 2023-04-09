from configparser import ConfigParser
import os.path

## Retrieve configuration from the config file
## Code from https://www.tutorialsbuddy.com/how-to-create-and-use-configuration-files-in-python

path_root = os.path.dirname(os.path.realpath(__file__))
path_configfile = dir_path+"/config.ini"
# check if the config file exists
exists = os.path.exists(path_configfile)
config = None
if exists:
    print("--------config.ini file found at ", path_configfile)
    config = ConfigParser()
    config.read(path_configfile)
else:
    print("---------config.ini file not found at ", path_configfile)

# Retrieve config details
config_database= config["DATABASE"]
config_keys = config["KEYS"]
config_varname = config["VARNAME"]