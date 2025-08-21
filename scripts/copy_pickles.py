import os
import shutil
import logging
import pickle
from pathlib import Path

from wwpdb.io.locator.PathInfo import PathInfo

pi = PathInfo()


def copy_pickles(source_dep, target_dep):
    test_pickles_location = os.path.join(os.path.dirname(pi.getDepositPath(dataSetId=source_dep)), "temp_files", "deposition-v-200", source_dep)
    copy_pickles_location = pi.getDirPath(dataSetId=target_dep, fileSource="pickles")

    for file_name in os.listdir(test_pickles_location):
        if file_name.endswith(".pkl"):
            source_path = os.path.join(test_pickles_location, file_name)
            destination_path = os.path.join(copy_pickles_location, file_name)

            try:
                shutil.copy(source_path, destination_path)
            except shutil.SameFileError:
                logging.warning("Source and destination paths are the same, skipping copy for %s", file_name)

    # copying the test pickle
    copy_pickles_location = pi.getDirPath(dataSetId=target_dep, fileSource="pickles")
    pklpath = Path(__file__).parent / "odtf" / "resources" / "pdbx_contact_author.pkl"
    logging.info("Copying pickle file %s to %s", pklpath, copy_pickles_location)
    shutil.copy(pklpath, copy_pickles_location)

    # writing the submitOK.pkl file
    for ppath in [copy_pickles_location, test_pickles_location]:
        with open(os.path.join(ppath, "submitOK.pkl"), "wb") as f:
            pickle.dump({
                'annotator_initials': 'TST',
                'date': '2025-06-24 10:53:30',
                'reason': 'Submission test'
            }, f)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python copy_pickles.py <source_dep> <target_dep>")
        sys.exit(1)

    source_dep = sys.argv[1]
    target_dep = sys.argv[2]
    copy_pickles(source_dep, target_dep)