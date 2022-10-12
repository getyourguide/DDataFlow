import os
import shutil

from ddataflow import DDataflow


def setup_project():
    config_file = DDataflow._DDATAFLOW_CONFIG_FILE
    path = os.path.dirname(os.path.realpath(__file__)) + "/ddataflow_config.py"

    shutil.copyfile(path, config_file)
    print(f"File {config_file} created in the current directory.")
