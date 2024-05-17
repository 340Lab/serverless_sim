import os
import re
import subprocess

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
CUR_F=os.path.basename(__file__)

# chdir to the directory of this script
os.chdir(CUR_FDIR)

# download the chromedriver and google-chrome install package manully

os.system(f'docker build -t serverless_sim_ui . --no-cache')

