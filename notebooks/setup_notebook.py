import os
from pathlib import Path


def set_path_to_root():
    root_dir = Path(os.path.abspath('')).parent
    os.chdir(root_dir)


set_path_to_root()
