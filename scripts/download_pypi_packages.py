#!/usr/bin/env python3.8

import argparse
import os
import json

from typing import Dict, Any
from urllib.request import urlretrieve
from urllib.error import HTTPError
from multiprocessing import Pool

argparser = argparse.ArgumentParser(
    prog="download_pypi_packages", description="Helper program to download PyPI packages",
)
argparser.add_argument(
    "-n", "--number", type=int, default=100, help="Number of packages to download"
)
argparser.add_argument(
    "-a", "--all", action="store_true", help="Download all packages listed in the json file"
)
argparser.add_argument(
    "--rm", action="store_true", help="Remove JSON after running"
)
argparser.add_argument(
    "-p", "--processes", type=int, default=1, help="Number of concurrent packages to download"
)


def load_json(filename: str) -> Dict[Any, Any]:
    with open(os.path.join("data", f"{filename}.json"), "r") as f:
        j = json.loads(f.read())
    return j


def remove_json(filename: str) -> None:
    path = os.path.join("data", f"{filename}.json")
    os.remove(path)


def download_package_json(package_name: str) -> None:
    url = f"https://pypi.org/pypi/{package_name}/json"
    try:
        urlretrieve(url, os.path.join("data", f"{package_name}.json"))
        return 0
    except HTTPError as e:
        print(e)
    return 1
        

def download_package_code(name: str, package_json: Dict[Any, Any]) -> None:
    source_index = -1
    for idx, url_info in enumerate(package_json["urls"]):
        if url_info["python_version"] == "source":
            source_index = idx
            break
    filename = package_json["urls"][source_index]["filename"]
    url = package_json["urls"][source_index]["url"]
    dl_path = os.path.join("data", "pypi", filename)
    if os.path.exists(dl_path):
        print(f"{name} already downloaded")
        return
    try:
        urlretrieve(url, dl_path)
    except HTTPError as e:
        print(e)


def analyze_package(package: Dict[Any, Any], remove: bool = False):
    package_name = package["project"]

    print(f"Downloading JSON Data for {package_name}... ", end="")
    if download_package_json(package_name):
        print("Failed downloading package data")
        continue
    print("Done")

    package_json = load_json(package_name)
    try:
        print(f"Dowloading and compressing package {package_name} ... ", end="")
        download_package_code(package_name, package_json)
        print("Done")
    except (IndexError, KeyError):
        print(f"Could not locate source for {package_name}")
        continue
    finally:
        if remove:
            remove_json(package_name)

def main() -> None:
    args = argparser.parse_args()
    number_packages = args.number
    all_packages = args.all

    top_pypi_packages = load_json("top-pypi-packages-365-days")
    if all_packages:
        top_pypi_packages = top_pypi_packages["rows"]
    elif number_packages >= 0 and number_packages <= 4000:
        top_pypi_packages = top_pypi_packages["rows"][:number_packages]
    else:
        raise AssertionError("Unknown value for NUMBER_OF_PACKAGES")

    try:
        os.mkdir(os.path.join("data", "pypi"))
    except FileExistsError:
        pass
    pool = Pool(args.processes)
    pool.imap_unordered(analyze_package, top_pypi_packages, args.remove)


if __name__ == "__main__":
    main()
