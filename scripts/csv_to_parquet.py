#!/usr/bin/env python
"""
A Python script to convert the ULB data from CSV files to a Parquet file.

It assumes CSV files are stored as "Campaigns", where a directory contains
multiple campaigns, each of which contains one subdirectory per experiment,
each of which contains one or more CSV files that belong to the same experiment.

It will write out a Parquet file for each experiment in the directory of its
campaign. It assumes if an experiment consists of multiple data files, their
names are in alphabetical order.

So if the input files are
    START_DIRECTORY
    └── Campaign1
        ├── Experiment1
        │   ├── Data1.csv
        │   └── Data2.csv
        └── Experiment2
            └── Data1.csv
The output will be at
    START_DIRECTORY
    └── Campaign1
        ├── Experiment1.parquet
        └── Experiment2.parquet
"""
import pandas  # Everyone uses Pandas as a full import so I will for consistency

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
from typing import List
from pandas import DataFrame  # Except for this, to make the type hinting prettier
from tqdm import tqdm

GLOB_DIRECTORY_PATTERN: str = "*"
GLOB_SUBDIRECTORY_PATTERN: str = "*"
GLOB_CSV_PATTERN: str = "*.[Cc][Ss][Vv]"  # Case-insensitive for the suffix.
PARQUET_COMPRESSION: str = "gzip"  # gzip is ~20% slower than the default 'snappy', but ~2x smaller
PARQUET_ENGINE: str = "fastparquet"  # For more options for this and compression, see pandas dataframe.to_parquet 


parser: ArgumentParser = ArgumentParser(
    prog="csv_to_parquet",
    description=__doc__,  # The docstring of this file
    formatter_class=RawDescriptionHelpFormatter,
)

parser.add_argument(
    '-dt', '--datetimes',
    type=str, help="Column in the CSVs to parse as datetimes."
)
parser.add_argument(
    '-i', '--index',
    type=str, help="Column in the CSVs to use as an index."
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
    '-hp', '--high-precision', action='store_true', default=False,
    help="Whether to store numeric data in the file as 64-bit floats and ints.\n"
         "By default, assumes that the CSVs are only accurate to 32 bit (~7dp).\n"
         "If set, the Parquet files will be about twice as large."
)
parser.add_argument(
    '-v', '--verbose', action='store_true', default=False,
    help="Whether to print out all the skipped directories."
)

parser_pattern = parser.add_argument_group('file pattern arguments')
parser_pattern.add_argument(
    '-c', '--csv-pattern',
    type=str, default=GLOB_CSV_PATTERN,
    help="Regular expression pattern to match for CSV filenames.\n"
         f"By default '{GLOB_CSV_PATTERN}'; could change to 'PSTc*.csv' or similar."
)
parser_pattern.add_argument(
    '-d', '--directory-pattern',
    type=str, default=GLOB_DIRECTORY_PATTERN,
    help="Regular expression pattern to match for 'campaign' directories.\n" 
         f"By default '{GLOB_DIRECTORY_PATTERN}'; could change to 'Campaign*' or similar."
)
parser_pattern.add_argument(
    '-s', '--subdirectory-pattern',
    type=str, default=GLOB_SUBDIRECTORY_PATTERN,
    help="Regular expression pattern to match for subdirectories.\n"
         f"By default '{GLOB_SUBDIRECTORY_PATTERN}'; could change to 'PSTc*' or similar."
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

print(
    f"Converting campaigns in '{arguments.start_directory}' "
    f"matching '{arguments.directory_pattern}' to Parquet"
)

# We'll track the sizes to see how good the compression is
original_sizes: int = 0
parquet_sizes: int = 0
num_files_converted: int = 0
num_files_skipped: int = 0

# We'll also store any directories we found that weren't valid
empty_directories: List[str] = []

# TQDM doesn't like printing errors within loops so we do this


# We use glob to get everything in the current directory matching our pattern
# then ignore everything that isn't a directory itself (or hidden!)
campaign_directories: List[Path] = [
    item for item in arguments.start_directory.glob(
        arguments.directory_pattern
    ) if item.is_dir() and not item.name.startswith('.')
]
campaign_directories_progress: tqdm = tqdm(
    campaign_directories, desc="Scanning", unit=" directory"
)

for campaign_directory in campaign_directories_progress:
    # We use glob again to find everything matching our pattern
    # and ignore anything that isn't a directory (e.g. the Parquet files!)
    experiment_directories: List[Path] = [
        item for item in campaign_directory.glob(
            arguments.subdirectory_pattern
            ) if item.is_dir() and not item.name.startswith('.')
    ]
    experiment_directories_progress: tqdm = tqdm(
        experiment_directories, desc=str(campaign_directory), leave=False, unit=" experiment"
    )

    for experiment_directory in experiment_directories_progress:
        experiment_directories_progress.set_postfix(
            {'current': experiment_directory.name}
        )
        
        # Don't overwrite unless we're ordered to!
        if experiment_directory.with_suffix('.parquet').exists() and not arguments.overwrite:
            num_files_skipped += 1
            continue

        # Get a sorted list of all the CSV files within this directory 
        csv_filenames: List[Path] = list(
            experiment_directory.glob(
                arguments.csv_pattern
            )
        )
        csv_filenames.sort()

        # If there *isn't* anything here, then let's move on
        if not csv_filenames:
            empty_directories.append(
                f"{experiment_directory}: No files matching '{arguments.csv_pattern}'"
            )
            continue  # We skip this directory and go to the next one

        # Count the original size of these files
        for csv_filename in csv_filenames:
            original_sizes += csv_filename.stat().st_size

        # Load all the dataframes we found
        try:
            dataframes: List[DataFrame] = [
                pandas.read_csv(
                    csv_filename, 
                    parse_dates=[arguments.datetimes] if arguments.datetimes else False,
                    infer_datetime_format=True,
                    index_col=arguments.index if arguments.index else None
                ) for csv_filename in csv_filenames
            ]
        except Exception as error:
            # If it didn't work, provide a nicer error
            raise Exception(
                f"Failed to read '{csv_filename}'." +
                (f" Is index column '{arguments.index}' correct?" if arguments.index else '') +
                (f" Is datetime column '{arguments.datetimes}' correct?" if arguments.datetimes else '')
            )

        # Concatenate all the dataframes we found into one
        dataframe: DataFrame = pandas.concat(dataframes)

        # Convert all the numeric columns to smaller, unless we know it's a good file
        if not arguments.high_precision:
            for column in dataframe.select_dtypes('float64'):
                dataframe[column] = dataframe[column].astype('float32')

            for column in dataframe.select_dtypes('int64'):
                dataframe[column] = dataframe[column].astype('int32')

        # Remove all the ' ' in the column names, and replace them with '_'
        dataframe.rename(
            columns={
                column: column.replace(' ', '_') for column in dataframe.columns
            },
            inplace=True  # Edit the current dataframe
        )

        # Save the dataframe back out to a Parquet file
        parquet_filename: Path = experiment_directory.with_suffix('.parquet')
        dataframe.to_parquet(
            parquet_filename, 
            index=False,  # The index is just row number so we don't need to duplicate it
            engine=arguments.parquet_engine,
            compression=arguments.parquet_compression
        )
        parquet_sizes += parquet_filename.stat().st_size 
        num_files_converted += 1
    
    if not experiment_directories:
        empty_directories.append(
            f"{campaign_directory}: No subdirectories matching "
            f"'{arguments.subdirectory_pattern}'"
        )

if not campaign_directories:    
    empty_directories.append(
        f"{arguments.directory}: No directories matching "
        f"'{arguments.directory_pattern}'"
    )

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

# Log empty directories if requested
if arguments.verbose:
    print('')
    print('\n'.join(empty_directories))
