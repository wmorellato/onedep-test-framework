import os
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

import pickle
import concurrent.futures
import hashlib
import logging
import shutil
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import click
import MySQLdb

from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from onedep_deposition.deposit_api import DepositApi
from onedep_deposition.enum import Country, FileType
from onedep_deposition.models import (DepositError, DepositStatus, EMSubType,
                                      ExperimentType)

from wwpdb.apps.deposit.auth.tokens import create_token
from wwpdb.apps.deposit.common.utils import parse_filename
from wwpdb.apps.deposit.depui.constants import uploadDict
from wwpdb.apps.deposit.main.archive import ArchiveRepository, LocalFileSystem
from wwpdb.apps.deposit.main.schemas import ExperimentTypes
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import (ConfigInfoAppBase,
                                              ConfigInfoAppCommon)

ORCID = "0000-0002-5109-8728"
PROD_SITE_ID = "PDBE_DEV" # ENTER THIS
PROD_CONFIG = ConfigInfo(siteId=PROD_SITE_ID)
DEBUG = False
test_dep_id = "D_8233000170"
file_ids = [366803, 366802, 366800]

api = DepositApi(api_key=create_token(ORCID, expiration_days=1/24), hostname="https://localhost:12000/deposition", ssl_verify=False)

# for f in file_ids:
#     api.update_metadata(test_dep_id, f, contour=0.01, spacing_x=1.0825, spacing_y=1.0825, spacing_z=1.0825, description="Uploaded from test script")

print(api.process(test_dep_id))
# print(api.get_status(test_dep_id))