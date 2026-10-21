

from datetime import datetime
import functools
import itertools
import ntpath
import os
import pyspark.sql.functions as func
import re
import zipfile

from . import common


def coalesce_paths_with_alias(paths):
    # coalesce returns the first column that is not null.
    return func.coalesce(*[func.col(path) for path in paths])



def flatten_tuples(t):
    for elem in t:
        if isinstance(elem, tuple):
            yield from flatten_tuples(elem)
        else:
            yield elem

def combine_any_paths(array):
    if not isinstance(array, list):
        raise TypeError("A list of paths should be supplied")

    if not all(isinstance(array_elem, list) for array_elem in array):
        raise TypeError("All elements within the list should be lists")

    all_paths_elements = []
    all_combinations = list(functools.reduce(lambda a, b : itertools.product(a, b), array))
    for combination in all_combinations:
        all_paths_elements.append(tuple(flatten_tuples(combination)))
    
    return [".".join(path_elems) for path_elems in all_paths_elements]




def remove_duplicates_from_landing_import_folder(config_path):
    """Remove Duplicate Files from Landing Import Folder.

    It might occur that a zip file was extracted so that both the ZIP file and the
    XML file are in the same folder. This would lead to multiple reads of the same
    data. Thus, this function checks for such duplicates and deletes the xml file.

    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    # Only an XML file that has an archive of the same name beside it is
    # removed. Counting the names that occur twice whatever their extension
    # and removing the XML of each would also fire on a pair that has no XML
    # at all -- an interrupted download leaves ``<file>.zip.part`` beside
    # ``<file>.zip`` -- and then fail on a file that is not there.
    #
    # os.listdir returns bare names, so they have to be joined to the folder
    # before they are tested: os.path.isfile resolves a bare name against the
    # working directory, which would answer False for every one of them.
    import_folder = common.path_landing_import(config_path)
    files_in_folder = [
        f for f in os.listdir(import_folder)
        if os.path.isfile(os.path.join(import_folder, f))
    ]
    archives = {
        os.path.splitext(f)[0] for f in files_in_folder if f.lower().endswith(".zip")
    }
    for file_i in files_in_folder:
        if file_i.lower().endswith(".xml") and os.path.splitext(file_i)[0] in archives:
            os.remove(os.path.join(import_folder, file_i))


def get_candidates_for_import(config_path):
    """Files for data import.
    
    Creates a list of files in the import folder, which are available for import.
    
    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    """
    # The extension is tested at the end of the name rather than anywhere in
    # it, so that the ``<file>.zip.part`` an interrupted download leaves
    # behind is not taken for an archive and imported.
    import_folder = common.path_landing_import(config_path)
    files_in_folder = [
        f for f in os.listdir(import_folder)
        if os.path.isfile(os.path.join(import_folder, f))
    ]
    xml_files = [f for f in files_in_folder if f.lower().endswith(".xml")]
    zip_files = [f for f in files_in_folder if f.lower().endswith(".zip")]
    return xml_files + zip_files

def get_dltins_data_date(file):
    filename = ntpath.basename(file)
    refdt = filename[7:15]
    return datetime.strptime(refdt, '%Y%m%d').strftime('%Y-%m-%d')

def get_fulins_data_date(file):
    filename = ntpath.basename(file)
    refdt = filename[9:17]
    return datetime.strptime(refdt, '%Y%m%d').strftime('%Y-%m-%d')

def get_fulcan_data_date(file):
    """The publication date a cancellations file is named after.

    The name is ``FULCAN_<YYYYMMDD>_<n>of<m>.zip``, so the date sits at the
    same offset as in a delta file. It is read out separately all the same,
    because the two are different kinds of file and a change to one naming
    convention should not silently move the other.
    """
    filename = ntpath.basename(file)
    refdt = filename[7:15]
    return datetime.strptime(refdt, '%Y%m%d').strftime('%Y-%m-%d')


#: Prefix of the partition directories of the Bronze layer. The Bronze layer
#: writes them and the Silver layer reads the reference dates off them.
PARTITION_PREFIX = "DATADT="


#: The names ESMA gives its files, e.g. ``DLTINS_20260906_01of02.zip`` or
#: ``FULINS_E_20260905_03of04.zip``. A full file is published in parts per
#: first letter of the CFI code, the other kinds in parts per day.
FILE_NAME = re.compile(
    r"^(?P<kind>FULINS|DLTINS|FULCAN)_(?:(?P<cfi>[A-Z])_)?(?P<date>\d{8})"
    r"_(?P<part>\d+)of(?P<parts>\d+)\.(?:zip|xml)$",
    re.IGNORECASE,
)


def parse_file_name(name):
    """Return kind, CFI letter, date, part and number of parts of an ESMA file.

    Returns ``None`` for a name that does not follow ESMA's convention.
    """
    match = FILE_NAME.match(ntpath.basename(name))
    if match is None:
        return None
    return {
        "kind": match["kind"].upper(),
        "cfi": match["cfi"],
        "date": datetime.strptime(match["date"], "%Y%m%d").date(),
        "part": int(match["part"]),
        "parts": int(match["parts"]),
    }


def missing_parts(names, kind, day):
    """Return what is missing for the files of ``kind`` published on ``day``.

    A file published in ``m`` parts is complete once parts 1 to ``m`` are all
    among ``names``. Applying a day from some of its parts would leave the
    history without the records of the others, and nothing downstream could
    tell, so a day is only as good as its least complete file.

    Parameters
    ----------
    names: list of str
        The names of the files that have been imported.
    kind: str
        ``'FULINS'``, ``'DLTINS'`` or ``'FULCAN'``.
    day: datetime.date
        The publication date.

    Returns
    -------
    list of str
        A description of every missing part, or of the whole day when no file
        of it is among ``names``. Empty when the day is complete.
    """
    groups = {}
    for name in names:
        parsed = parse_file_name(name)
        if parsed is None or parsed["kind"] != kind or parsed["date"] != day:
            continue
        parts, seen = groups.setdefault(parsed["cfi"], [0, set()])
        groups[parsed["cfi"]][0] = max(parts, parsed["parts"])
        seen.add(parsed["part"])

    spelled = day.strftime("%Y%m%d")
    if not groups:
        return [f"{kind} of {spelled}: no part"]

    missing = []
    for cfi, (parts, seen) in sorted(groups.items(), key=lambda g: g[0] or ""):
        prefix = f"{kind}_{cfi}_" if cfi else f"{kind}_"
        missing.extend(
            f"{prefix}{spelled}_{part:02d}of{parts:02d}"
            for part in range(1, parts + 1)
            if part not in seen
        )
    return missing


def unimported_listed_files(names, listed):
    """Return the files of ``listed`` that are not among ``names``.

    :func:`missing_parts` judges a file by the parts that arrived, and a full
    file is published in parts per first letter of the CFI code, so a letter
    of which no part arrived leaves no trace it could notice. Only the list of
    files the register published can say that such a letter is missing, and
    this compares the two. Names are compared without their extension, since
    an archive and the XML extracted from it are the same file.

    Parameters
    ----------
    names: list of str
        The names of the files that have been imported.
    listed: list of str
        The names of the files the register listed.

    Returns
    -------
    list of str
        The listed files that have not been imported, without their
        extension and in order. Empty when every one of them has.
    """
    def stem(name):
        return os.path.splitext(ntpath.basename(name))[0]

    imported = {stem(name).upper() for name in names}
    return sorted({stem(name) for name in listed if stem(name).upper() not in imported})


def unzip_and_get_names(config_path, zip_file):
    """Unzip file and get the name of its content.
    
    A helper function, which unzips a given file and gives the name of its content.
    The content is assumed to be only one file.
    
    Parameters
    ----------
    config_path: str
        Path to the configuration file, where paths are specified. This must be a yaml-file.
    zip_file: str
        Name of the zip-file.
    """
    with zipfile.ZipFile(os.path.join(common.path_landing_import(config_path), zip_file), 'r') as zip_ref:
        zip_ref.extractall(common.path_landing_import(config_path))
    return zip_ref.namelist()[0]

def read_first_n_chars(filename, n):
    """Read first N characters of a text file.

    Used to extract the first n characters of a large xml file.
    
    Parameters
    ----------
    filename: str
        Path to the xml file.
    n: int
        Number of characters that should be loaded.
    """
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read(n)
        
def extract_between_keywords(text, node):
    """Get text between two keywords.

    Used in combination with 'read_first_n_chars' to extract the securitisation ID and
    cut-off-date from a large xml file.
    
    Parameters
    ----------
    text: str
        Text extracted using 'read_first_n_chars'.
    node: str
        Name of the node that should be extracted.
    """
    start_keyword = "<" + node + ">"
    end_keyword = "</" + node + ">"
    
    # Split by start keyword and take everything after it
    after_start = text.split(start_keyword, 1)
    if len(after_start) < 2:
        return None  # Start keyword not found
    
    # Split by end keyword and take everything before it
    before_end = after_start[1].split(end_keyword, 1)
    return before_end[0] if before_end else None


#: What this module offers. Helpers shared by the stages: the files in the import folder,
#: what their names say and how they are unpacked.
__all__ = [
    "FILE_NAME",
    "PARTITION_PREFIX",
    "coalesce_paths_with_alias",
    "combine_any_paths",
    "extract_between_keywords",
    "flatten_tuples",
    "get_candidates_for_import",
    "get_dltins_data_date",
    "get_fulcan_data_date",
    "get_fulins_data_date",
    "missing_parts",
    "parse_file_name",
    "read_first_n_chars",
    "remove_duplicates_from_landing_import_folder",
    "unimported_listed_files",
    "unzip_and_get_names",
]
