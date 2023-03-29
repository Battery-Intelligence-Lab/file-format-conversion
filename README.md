# file-format-conversion
A set of scripts to convert between common file formats for data. For example, we use them to convert time series data for batteries from CSV to Parquet.

This repository contains some Python and Matlab scripts for converting from CSV, NPY and MAT files to the Parquet file format. More information about Parquet can be found here: https://www.databricks.com/glossary/what-is-parquet

See below for instructions for 1) Matlab and 2) Python users.

# 1) Installation for Matlab users

Clone the repository or just download the csv_to_parquet.m or mat_to_parquet.m function as required. Add the function to your Matlab path and open to read the documentation and possible options. Navigate to the main folder containing your folders and subfolders, or define the `options.StartDirectory`, before running the code.

Tip: When loading an existing Parquet file into Matlab, if the Date/Time column shows as a numeric value rather than a `datetime`, try converting the column using:

```
x1 = parquetread('FILENAME.parquet');
x1.Time = x1.Time/1e9; % nanoseconds to seconds
x1.Time = datetime(x1.Time,'ConvertFrom','posixtime');
```


# 2) Installation for Python users

Clone the repository, and install it through pip as:
```
git clone git@github.com:Battery-Intelligence-Lab/file-format-conversion.git
pip install file-format-conversion/
```

## Windows
If you're running through Powershell, and run into problems with Python popping up in a window and then closing *immediately* after finishing, try running:
```
$env:PATHEXT += ';.PY'
```

# Usage

You can now run the scripts through the terminal as:
```
csv_to_parquet.py
```
and
```
npy_to_parquet.py
```

The full descriptions of `csv_to_parquet.py` are accessible via `--help`, and listed below.

## CSV to Parquet
```
usage: csv_to_parquet [-h] [-dt DATETIMES] [-i INDEX] [-o] [-hp] [-v] [-c CSV_PATTERN]
                      [-d DIRECTORY_PATTERN] [-s SUBDIRECTORY_PATTERN] [-pe PARQUET_ENGINE]
                      [-pc PARQUET_COMPRESSION]
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
  START_DIRECTORY       Path to the directory to search. By default searches the current working
                        directory.

optional arguments:
  -h, --help            show this help message and exit
  -dt DATETIMES, --datetimes DATETIMES
                        Column in the CSVs to parse as datetimes.
  -i INDEX, --index INDEX
                        Column in the CSVs to use as an index.
  -o, --overwrite       Whether to overwrite any existing Parquet files. By default will skip
                        subdirectories rather than overwrite.
  -hp, --high-precision
                        Whether to store numeric data in the file as 64-bit floats and ints. By
                        default, assumes that the CSVs are only accurate to 32 bit (~7dp). If set,
                        the Parquet files will be about twice as large.
  -v, --verbose         Whether to print out all the skipped directories.

file pattern arguments:
  -c CSV_PATTERN, --csv-pattern CSV_PATTERN
                        Regular expression pattern to match for CSV filenames. By default
                        '*.[Cc][Ss][Vv]'; could change to 'PSTc*.csv' or similar.
  -d DIRECTORY_PATTERN, --directory-pattern DIRECTORY_PATTERN
                        Regular expression pattern to match for 'campaign' directories. By default
                        '*'; could change to 'Campaign*' or similar.
  -s SUBDIRECTORY_PATTERN, --subdirectory-pattern SUBDIRECTORY_PATTERN
                        Regular expression pattern to match for subdirectories. By default '*'; could
                        change to 'PSTc*' or similar.

parquet arguments:
  -pe PARQUET_ENGINE, --parquet-engine PARQUET_ENGINE
                        Engine to use for Parquet file write. By default 'fastparquet'; takes all
                        options from Pandas to_parquet.
  -pc PARQUET_COMPRESSION, --parquet-compression PARQUET_COMPRESSION
                        Type of compression to use for Parquet files. By default 'gzip'; takes all
                        options from Pandas to_parquet.
```

## NPY to Parquet
```
usage: npy_to_parquet [-h] [-o] [-n NPY_PATTERN] [-f FORMAT] [-pe PARQUET_ENGINE]
                      [-pc PARQUET_COMPRESSION]
                      [START_DIRECTORY]

A Python script to convert the data from npy files to a Parquet file.

It assumes the npy files are all stored in a single directory, and match a 
specific format, or one provided by the user in a formatting file.
It will write a single Parquet file for each npy file.

The default format is: Time (unix seconds), Current, Voltage, Temperature
If this doesn't work, the user will be prompted to create a new format file.

positional arguments:
  START_DIRECTORY       Path to the directory to search. By default searches the current working
                        directory.

optional arguments:
  -h, --help            show this help message and exit
  -o, --overwrite       Whether to overwrite any existing Parquet files. By default will skip
                        subdirectories rather than overwrite.
  -n NPY_PATTERN, --npy-pattern NPY_PATTERN
                        Regular expression pattern to match for numpy filenames. By default
                        '*.[Nn][Pp][Yy]'; could change to 'PSTc*.npy' or similar.
  -f FORMAT, --format FORMAT
                        Numpy column format. Specifies names of columns, datetime format, and
                        precision. If not provided, uses a default format. If that doesn't work, will
                        prompt user to create their own.

parquet arguments:
  -pe PARQUET_ENGINE, --parquet-engine PARQUET_ENGINE
                        Engine to use for Parquet file write. By default 'fastparquet'; takes all
                        options from Pandas to_parquet.
  -pc PARQUET_COMPRESSION, --parquet-compression PARQUET_COMPRESSION
                        Type of compression to use for Parquet files. By default 'gzip'; takes all
                        options from Pandas to_parquet.
```
