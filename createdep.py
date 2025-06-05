import logging
import os
import shutil
import time

import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

from onedep_deposition.deposit_api import DepositApi
from onedep_deposition.enum import Country, FileType
from wwpdb.apps.deposit.auth.tokens import create_token

ORCID = "0000-0002-5109-8728"
api = DepositApi(api_key=create_token(ORCID, expiration_days=1/24), hostname="https://localhost:12000/deposition", ssl_verify=False)

email = "wbueno@ebi.ac.uk"
users = [ORCID]
country = Country.UK
password = "password"

model_file = "/wwpdb/onedep/deployments/dev/source/py-wwpdb_apps_deposit/2gc2.cif"
sf_file = "/wwpdb/onedep/deployments/dev/source/py-wwpdb_apps_deposit/2gc2-sf.cif"
# model_file = "/wwpdb/onedep/deployments/dev/source/py-wwpdb_apps_deposit/coord.cif.gz"
# sf_file = "/wwpdb/onedep/deployments/dev/source/py-wwpdb_apps_deposit/sf.mtz.gz"

# deposition = api.create_xray_deposition(email, users, country, password)
# dep_id = deposition.dep_id
dep_id = "D_8233000178"

# response = api.upload_file(dep_id, model_file, FileType.MMCIF_COORD, overwrite=False)
# print(response)
# response = api.upload_file(dep_id, sf_file, FileType.CRYSTAL_MTZ, overwrite=False)
# print(response)

# print(api.get_status(dep_id))

# copy_elements = {"copy_contact": False, "copy_authors": False, "copy_citation": False, "copy_grant": False, "copy_em_exp_data": False}
# response = api.process(dep_id)
# print(response)

status = ""
while status not in ("finished", "exception", "failed"):
    time.sleep(3)
    response = api.get_status(dep_id)
    status = response.status
    print(response)

