import os
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

from wwpdb.apps.deposit.auth.tokens import create_token
from wwpdb.apps.deposit.common.utils import parse_filename
from wwpdb.apps.deposit.depui.constants import uploadDict
from wwpdb.apps.deposit.main.archive import ArchiveRepository, LocalFileSystem
from wwpdb.apps.deposit.main.schemas import ExperimentTypes

for _, value in uploadDict.items():
    for k2, v2 in value.items():
        for k3, v3 in v2.items():
            print(f"{v3[1]}.{v3[2]}")
