import os
import re
import argparse


def get_files(current_dir, filename_pattern=".*"):
    """Files with the identical basenames are overwritten"""
    files_dict = {}
    for root, dirs, files in os.walk(current_dir):
        files_dict.update(
            {filename: os.path.join(root, filename) for filename in files if re.match(filename_pattern, filename)}
        )
    return files_dict


def normalize_args(args, skip_list=[]):
    """Converts all relative path arguments to absolute ones relatively to the current working directory"""
    normalized_args = {}
    for key,value in args.__dict__.items():
        if key not in skip_list:
            normalized_args[key] = value if not value or os.path.isabs(value) else os.path.normpath(os.path.join(os.getcwd(), value))
        else:
            normalized_args[key]=value
    return argparse.Namespace (**normalized_args)


def get_folder(abs_path, permissions=0o0775, exist_ok=True):
    try:
        os.makedirs(abs_path, mode=permissions)
    except os.error as ex:
        if not exist_ok:
            raise
    return abs_path


def export_to_file(data, filename):
    get_folder(os.path.dirname(filename))
    with open(filename, 'w') as output_stream:
        output_stream.write(data)