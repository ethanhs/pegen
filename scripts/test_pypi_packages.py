#!/usr/bin/env python3.8

import argparse
import os
import glob
import tarfile
import zipfile
import shutil
import sys
from multiprocessing import Pool
from functools import partial
from typing import Generator, Any, Optional

sys.path.insert(0, ".")
from pegen import build
from scripts import test_parse_directory

argparser = argparse.ArgumentParser(
    prog="test_pypi_packages", description="Helper program to test parsing PyPI packages",
)
argparser.add_argument(
    "-t", "--tree", action="count", help="Compare parse tree to official AST", default=0
)
argparser.add_argument(
    "-p", "--processes", type=int, default=1, help="Number of concurrent packages to check"
)

extension = build.build_parser_and_generator(
    "data/simpy.gram", "pegen/parse.c", compile_extension=True
)

def get_packages() -> Generator[str, None, None]:
    all_packages = (
        glob.glob("./data/pypi/*.tar.gz")
        + glob.glob("./data/pypi/*.zip")
        + glob.glob("./data/pypi/*.tgz")
    )
    for package in all_packages:
        yield package


def extract_files(filename: str) -> None:
    savedir = os.path.join("data", "pypi")
    if tarfile.is_tarfile(filename):
        tarfile.open(filename).extractall(savedir)
    elif zipfile.is_zipfile(filename):
        zipfile.ZipFile(filename).extractall(savedir)
    else:
        raise ValueError(f"Could not identify type of compressed file {filename}")


def find_dirname(package_name: str) -> Optional[str]:
    for name in os.listdir(os.path.join("data", "pypi")):
        full_path = os.path.join("data", "pypi", name)
        if os.path.isdir(full_path) and name in package_name:
            return full_path


def run_tests(dirname: str, tree: int, extension: Any) -> int:
    return test_parse_directory.parse_directory(
        dirname,
        "data/simpy.gram",
        verbose=False,
        excluded_files=[
            "*/failset/*",
            "*/failset/**",
            "*/failset/**/*",
            "*/test2to3/*",
            "*/test2to3/**/*",
            "*/bad*",
            "*/lib2to3/tests/data/*",
        ],
        skip_actions=False,
        tree_arg=tree,
        short=True,
        extension=extension,
    )

def analyze_package(package: str, tree: bool = True):
    print(f"Extracting files from {package}... ", end="")
    try:
        extract_files(package)
        print("Done")
    except ValueError as e:
        print(e)
        return

    print(f"Trying to parse all python files ... ")
    dirname = find_dirname(package)
    if dirname is None:
        print(f"Package {package} is a single file package")
        return
    try:
        status = run_tests(dirname, tree, extension)
    except Exception as e:
        print(f"Exception encountered in analyzing {package}:\n")
        print(e)
        status = 1
    if status == 0:
        print("Done")
        try:
            shutil.rmtree(dirname)
        except Exception:
            pass
    else:
        print(f"Failed to parse {dirname}")

def safe_analyze_package(package: str, tree: bool = True):
    try:
        analyze_package(package, tree)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    with Pool(16) as pool:
        for _ in pool.imap_unordered(safe_analyze_package, get_packages()):
            pass
