import os
from typing import Callable, List
import os
import re
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

from wwpdb.apps.deposit.common.utils import parse_filename
from wwpdb.apps.deposit.main.archive import ArchiveRepository

from wwpdb.io.locator.DataReference import DataFileReference

from odtf.archive import FileFinder

for ct in ("", "upload", "upload-convert"):
    files = FileFinder("/tmp/onedep_test__r9oginv/base/D_8233000175/data/").find_single(
        content_type="model",
        format="pdbx",
        version="original",
        milestone=ct,
    )

    print(files)
