from setuptools import setup

setup(
    name = "file-format-conversion",
    version = "0.0.1",
    description = ("Scripts to convert battery files to Parquet."),
    license = "MIT",
    scripts=[
        "scripts/csv_to_parquet.py",
        "scripts/npy_to_parquet.py"
    ],
    install_requires=[
        'pyyaml',
        "numpy",
        "pandas",
        "fastparquet",
        "tqdm"
    ],
    long_description=open('README.md').read(),
)