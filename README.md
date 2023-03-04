# file-format-conversion
A set of scripts to convert between common battery data file formats.

This repository will contain Python scripts and instructions for converting from CSV and NPY files to the Parquet file format. More information about Parquet can be found here: https://www.databricks.com/glossary/what-is-parquet


# Installation

Clone the repository, and install it through pip as:
```
git clone git@github.com:Battery-Intelligence-Lab/file-format-conversion.git
pip install file-format-conversion/
```


# Usage

You can now run the scripts through the terminal as:
```
csv_to_parquet.py
```
There is also a very basic `npy_to_parquet.py` file, usable in the same way.

The full description of `csv_to_parquet.py` is accessible via `--help`:
```
usage: csv_to_parquet [-h] [-f] [-p] [-v] [-c CSV_PATTERN] [-d DIRECTORY_PATTERN] [-s SUBDIRECTORY_PATTERN]
                      [-pe PARQUET_ENGINE] [-pc PARQUET_COMPRESSION]
                      [START_DIRECTORY]

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

positional arguments:
  START_DIRECTORY       Path to the directory to search. By default searches the current working directory.

optional arguments:
  -h, --help            show this help message and exit
  -f, --force           Whether to overwrite any existing Parquet files. By default will skip subdirectories rather
                        than overwrite.
  -p, --high-precision  Whether to store numeric data in the file as 64-bit floats and ints. By default, assumes that
                        the CSVs are only accurate to 32 bit (~7dp). If set, the Parquet files will be about twice as
                        large.
  -v, --verbose         Whether to print out all the skipped directories.

file pattern arguments:
  -c CSV_PATTERN, --csv-pattern CSV_PATTERN
                        Regular expression pattern to match for CSV filenames. By default '*.[Cc][Ss][Vv]'; could
                        change to 'PSTc*' or similar.
  -d DIRECTORY_PATTERN, --directory-pattern DIRECTORY_PATTERN
                        Regular expression pattern to match for 'campaign' directories. By default '*'; could change
                        to 'Campaign*' or similar.
  -s SUBDIRECTORY_PATTERN, --subdirectory-pattern SUBDIRECTORY_PATTERN
                        Regular expression pattern to match for subdirectories. By default '*'; could change to
                        'PSTc*' or similar.

parquet arguments:
  -pe PARQUET_ENGINE, --parquet-engine PARQUET_ENGINE
                        Engine to use for Parquet file write. By default 'fastparquet'; takes all options from Pandas
                        to_parquet.
  -pc PARQUET_COMPRESSION, --parquet-compression PARQUET_COMPRESSION
                        Type of compression to use for Parquet files. By default 'gzip'; takes all options from Pandas
                        to_parquet.
```
