import os
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

from odtf.archive import RemoteFetcher
from odtf.models import RemoteArchive, LocalArchive


def test_fetcher():
    """Test the fetcher function."""
    remote_archive = RemoteArchive(host="pdb-002.ebi.ac.uk", user="w3_pdb05", site_id="PDBE_PROD", key_file="/home/onedep/.ssh/id_ebi")
    local_archive = LocalArchive(site_id="PDBE_DEV")

    fetcher = RemoteFetcher(remote_archive, local_archive)
    fetcher.fetch(dep_id="D_1292109328", repository="deposit", force=True)

    # assert result is not None
    # assert isinstance(result, EntryStatus)
    # assert result.status == "pending"


from wwpdb.io.locator.PathInfo import PathInfo
pi = PathInfo(siteId="PDBE_DEV")
dep_id = "D_8233000142"
print(pi.getDirPath(dataSetId="D_8233000142", fileSource="deposit"))
print(pi.getDirPath(dataSetId="D_8233000142", fileSource="pickles"))
print(os.path.join(os.path.dirname(pi.getDepositPath(dataSetId=dep_id)), "temp_files", "deposition-v-200", dep_id))