
#############################################
# Author: Piyush Bijwal
#############################################


import requests
import datetime
import time
import glob
import pandas as pd
from sqlalchemy import create_engine as ce
import os

def echo_msg(t_str):
		print(datetime.datetime.now().strftime("%Y%m%d %H%M%S")+'::: '+str(t_str))

