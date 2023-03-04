#!/usr/bin/env python
"""
A Python script to convert the data from npy files to a Parquet file.

It assumes the npy files are all stored in a single directory, and match a 
specific format, or one provided by the user in a formatting file.
It will write a single Parquet file for each npy file.

The default format is: Time (unix seconds), Current, Voltage, Temperature
If this doesn't work, the user will be prompted to create a new format file.
"""
import numpy  # Everyone uses Numpy as a full import so I will for consistency
import pandas  # Everyone uses Pandas as a full import so I will for consistency

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
from typing import List
from yaml import safe_load
from numpy.typing import NDArray
from pandas import DataFrame  # Except for this, to make the type hinting prettier
from tqdm import tqdm

GLOB_NPY_PATTERN: str = "*.[Nn][Pp][Yy]"  # Case-insensitive for the suffix.
PARQUET_COMPRESSION: str = "gzip"  # gzip is ~20% slower than the default 'snappy', but ~2x smaller
PARQUET_ENGINE: str = "fastparquet"  # For more options for this and compression, see pandas dataframe.to_parquet 
NPY_FORMAT_DEFAULT: str = \
"""columns: 
  - Time
  - Current
  - Voltage
  - Temperature
date_column:
  Time: s
float32: False"""

parser: ArgumentParser = ArgumentParser(
    prog="npy_to_parquet",
    description=__doc__,  # The docstring of this file
    formatter_class=RawDescriptionHelpFormatter,
)
parser.add_argument(
    'start_directory', metavar="START_DIRECTORY", nargs='?',
    type=Path, default=Path("."),
    help="Path to the directory to search.\n"
         "By default searches the current working directory."
)
parser.add_argument(
    '-o', '--overwrite', action='store_true', default=False,
    help="Whether to overwrite any existing Parquet files.\n"
         "By default will skip subdirectories rather than overwrite."
)
parser.add_argument(
    '-n', '--npy-pattern',
    type=str, default=GLOB_NPY_PATTERN,
    help="Regular expression pattern to match for numpy filenames.\n"
         f"By default '{GLOB_NPY_PATTERN}'; could change to 'PSTc*.npy' or similar."
)
parser.add_argument(
    '-f', '--format',
    type=Path,
    help="Numpy column format. Specifies names of columns, datetime format, and precision.\n"
         f"If not provided, uses a default format.\n"
         "If that doesn't work, will prompt user to create their own."
)

parser_parquet = parser.add_argument_group('parquet arguments')
parser_parquet.add_argument(
    '-pe', '--parquet-engine',
    default=PARQUET_ENGINE, type=str,
    help="Engine to use for Parquet file write.\n"
         f"By default '{PARQUET_ENGINE}'; takes all options from Pandas to_parquet."
)
parser_parquet.add_argument(
    '-pc', '--parquet-compression', 
    default=PARQUET_COMPRESSION, type=str,
    help="Type of compression to use for Parquet files.\n"
         f"By default '{PARQUET_COMPRESSION}'; takes all options from Pandas to_parquet."
)
arguments = parser.parse_args()

# Do some extra processing of the arguments
if arguments.format:
    npy_format: dict = safe_load(open(arguments.format))
else:
    npy_format: dict = safe_load(NPY_FORMAT_DEFAULT)

print(
    f"Converting files in '{arguments.start_directory}' "
    f"matching '{arguments.npy_pattern}' to Parquet. "
)

# We'll track the sizes to see how good the compression is
original_sizes: int = 0
parquet_sizes: int = 0
num_files_converted: int = 0
num_files_skipped: int = 0

# We use glob to get all '.npy' files in the starting directory
npy_filenames: List[Path] = [
    item for item in arguments.start_directory.glob(
        arguments.npy_pattern
    )
]
npy_filenames_progress: tqdm = tqdm(
    npy_filenames, desc="Scanning", unit=" files"
)

for npy_filename in npy_filenames_progress:
    npy_filenames_progress.set_postfix(
        {'current': npy_filename.name}
    )
    
    # Don't overwrite unless we're ordered to!
    if npy_filename.with_suffix('.parquet').exists() and not arguments.overwrite:
        num_files_skipped += 1
        continue

    # Count the original size of the file
    original_sizes += npy_filename.stat().st_size

    # We load the file, but it's *just* raw numbers
    array: NDArray = numpy.load(npy_filename)
    
    # So we store the columns in a dataframe, with the correct names
    # and correct the time column to a datetime
    try:
        dataframe: DataFrame = DataFrame(
            {
                name: array[:, idx] for idx, name in enumerate(npy_format['columns'])
            }
        )

        for date_name, date_units in npy_format['date_column'].items():
            dataframe[date_name] = pandas.to_datetime(
                dataframe[date_name], unit=date_units
            )
    except:
        format_file: Path = Path(
            npy_filename.with_name(
                npy_filename.name + '_format.yml'
            )
        )
        format_file.write_text(NPY_FORMAT_DEFAULT)
        raise Exception(
            f"'{npy_filename}' does not fit the expected format. "
            f"Creating a new format file '{format_file}'. Edit it to reflect the file format, "
            f"then re-run using '-f FORMAT_FILE_NAME'."
        )

    # Save the dataframe back out to a Parquet file
    parquet_filename = npy_filename.with_suffix('.parquet')
    dataframe.to_parquet(
        parquet_filename, 
        index=False,  # The index is just row number so we don't need to duplicate it
        engine=arguments.parquet_engine,
        compression=arguments.parquet_compression
    )
    parquet_sizes += parquet_filename.stat().st_size 
    num_files_converted += 1

# Give final feedback
if num_files_converted:
    print(
        f"{num_files_converted} converted file(s) are smaller " 
        f"by a factor of {original_sizes / parquet_sizes}"
    )
elif not arguments.overwrite and num_files_skipped:
    print(
        f"No files converted, but {num_files_skipped} pre-existing Parquet file(s) skipped"
    )
else:
    print(
        "No files converted."
    )
