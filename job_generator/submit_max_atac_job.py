import sys
import os
import argparse
import pandas as pd
import subprocess
from json import dumps
from job_generator.utils.utils import normalize_args


def arg_parser():
    general_parser = argparse.ArgumentParser()
    general_parser.add_argument("-m", "--metadata",    help="Path to metadata file",                         required=True)
    general_parser.add_argument("-d", "--dag",         help="Dag id",                                        required=True)
    general_parser.add_argument("-i", "--indices",     help="Path to indices folder",                        required=True)
    general_parser.add_argument("-b", "--blacklisted", help="Path to blacklisted regions file",              required=True)
    general_parser.add_argument("-c", "--chromlength", help="Path to chromosome length file",                required=True)
    general_parser.add_argument("-g", "--genome",      help="Path to genome FASTA file",                     required=True)
    general_parser.add_argument("-n", "--number",      help="Limit number of experiments to submit",         type=int)
    general_parser.add_argument("-o", "--output",      help="Path to be used as output_folder in job files", required=True)
    return general_parser


def get_metadata(metadata_file):
    return pd.read_table(metadata_file, index_col=0, comment='#').to_dict(orient="index")


def download_file(file_url):
    filename = os.path.join(os.getcwd(), os.path.basename(file_url))
    if os.path.isfile(filename):
        raise Exception("File already exists")
    params = ["wget", "-q", "--show-progress", file_url]
    env = os.environ.copy()
    subprocess.run(params, env=env)
    return filename


def trigger_dag(job, run_id, dag_id):
    params = ["airflow", "trigger_dag", "-r", run_id, "-c", dumps(job), dag_id]
    env = os.environ.copy()
    print("Trigger dag", params)
    subprocess.run(params, env=env)


def submit_jobs (args, metadata):
    expreriments = []
    for k, v in metadata.items():
        f = k
        s = v["Paired with"]
        if (f,s) in expreriments or (s,f) in expreriments:
            continue
        expreriments.append((f,s))
    count = 0
    for f, s in expreriments:
        if count >= args.number:
            continue
        print("\nProcess", f,s)
        f_url = metadata[f]["File download URL"]
        s_url = metadata[s]["File download URL"]
        print("Downloading FASTQ files:\n", f_url, "\n", s_url)
        try:
            f_location = download_file(f_url)
            s_location = download_file(s_url)
            run_id = f + "_" + k
            job_template = {
                "job": {
                    "fastq_file_1": {
                        "class": "File",
                        "location": f_location
                    },
                    "fastq_file_2": {
                        "class": "File",
                        "location": s_location
                    },
                    "indices_folder": {
                        "class": "Directory",
                        "location": args.indices
                    },
                    "blacklisted_regions_bed": {
                        "class": "File",
                        "location": args.blacklisted
                    },
                    "chrom_length_file": {
                        "class": "File",
                        "location": args.chromlength
                    },
                    "genome_size": "2.7e9",
                    "genome_fasta_file": {
                        "class": "File",
                        "location": args.genome
                    },
                    "output_folder": os.path.join(args.output, run_id),
                    "threads": 4
                }
            }
            print(dumps(job_template, indent = 4))
            trigger_dag(job_template, run_id, args.dag)
            count = count + 1
        except Exception as err:
            print("Failed to submit job", err)


def main(argsl=None):
    if argsl is None:
        argsl = sys.argv[1:]
    args,_ = arg_parser().parse_known_args(argsl)
    args = normalize_args(args, ["dag", "number"])

    metadata = get_metadata(args.metadata)

    submit_jobs(args, metadata)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))