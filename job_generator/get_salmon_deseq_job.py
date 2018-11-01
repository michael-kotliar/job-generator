import sys
import argparse
import os
import uuid
import pandas as pd
import re
from json import dumps
from job_generator.utils.utils import normalize_args, get_files, export_to_file


def arg_parser():
    general_parser = argparse.ArgumentParser()
    general_parser.add_argument("-m", "--metadata",  help="Path to metadata file", required=True)
    general_parser.add_argument("-f", "--fastq",     help="Path to FASTQ file folder", required=True)
    general_parser.add_argument("-i", "--indices",   help="Path to indices folder", required=True)
    general_parser.add_argument("-t", "--transform", help="Path to Tx to Gene transform file", required=True)
    general_parser.add_argument("-p", "--threads",   help="Number of threads to use", default=1)
    general_parser.add_argument("-o", "--output",    help="Path to be used as output_folder in job file", required=True)
    general_parser.add_argument("-u", "--uid",       help="Experiment unique ID. Default: random uuid", default=str(uuid.uuid4()))
    general_parser.add_argument("-w", "--workflow",  help="Path to workflow file", required=True)
    general_parser.add_argument("-r", "--result",    help="Output folder to put job file")
    return general_parser


def get_metadata(metadata_file):
    return pd.read_table(metadata_file, index_col=0).to_dict(orient="index")


def generate_jobs (args, metadata, filelist):
    job = {
        "fastq_file_upstream": [],
        "fastq_file_downstream": [],
        "category": [],
        "indices_folder": {
            "class": "File",
            "location": args.indices
        },
        "tx_to_gene_file": {
            "class": "File",
            "location": args.transform
        },
        "deseq_filename": args.uid,
        "threads": args.threads,
        "workflow": args.workflow,
        "output_folder": args.output,
        "uid": args.uid
    }

    for key, value in metadata.items():
        selected_files = sorted([path for filename, path in filelist.items() if re.match(key, filename)])
        job["fastq_file_upstream"].append(selected_files[0])
        job["fastq_file_downstream"].append(selected_files[1])
        job["category"].append(value["category"])

    return job


def main(argsl=None):
    if argsl is None:
        argsl = sys.argv[1:]
    args,_ = arg_parser().parse_known_args(argsl)
    args = normalize_args(args, ['uid','threads'])
    if not args.result:
        args.result = os.path.join(os.getcwd(), args.uid + ".json")
    fastq_files = get_files(args.fastq, ".*fastq.*")
    metadata = get_metadata(args.metadata)
    job = generate_jobs(args, metadata, fastq_files)
    export_to_file(dumps(job, indent=4), args.result)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))