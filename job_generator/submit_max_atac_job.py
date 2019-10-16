import sys
import os
import argparse
import pandas as pd
import subprocess
from json import dumps
from job_generator.utils.utils import normalize_args


INDEX_COLUMNS = ["Experiment accession", "Biological replicate(s)", "Paired end", "Technical replicate"]
EXP_COLUMNS = ["Experiment accession", "Biological replicate(s)"]
FILTER_COLUMN = "Run type"
FILTER_VALUE = "paired-ended"
CWD = os.getcwd()


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
    raw_data = pd.read_table(metadata_file, index_col=INDEX_COLUMNS, comment='#')
    return raw_data.loc[raw_data[FILTER_COLUMN] == FILTER_VALUE].sort_index()


def download_files(files_df, prefix):
    combined_filepath = os.path.join(CWD, prefix + ".fastq.gz")
    if os.path.isfile(combined_filepath):
        raise Exception(f"""File {combined_filepath} already exists""")
    for file_url in files_df["File download URL"]:
        current_filename = file_url.split("/")[-1]
        current_filepath = os.path.join(CWD, current_filename)
        if os.path.isfile(current_filepath):
            params = " ".join(["cat", current_filepath, ">>", combined_filepath])
        else:
            params = " ".join(["wget", "-O", "-", "-q", "--show-progress", file_url, ">>", combined_filepath])
        env = os.environ.copy()
        print("\n  Run", params)
        try:
            subprocess.run(params, env=env, shell=True, check=True)
        except Exception as err:
            os.remove(combined_filepath)
            raise err
    return combined_filepath


def trigger_dag(job, run_id, dag_id):
    params = ["airflow", "trigger_dag", "-r", run_id, "-c", dumps(job), dag_id]
    env = os.environ.copy()
    print("Trigger dag", params)
    subprocess.run(params, env=env)


def properly_paired(first_files, second_files):
    def equal(f,s):
        return (f["Paired with"].equals(s["File accession"])) and (s["Paired with"].equals(f["File accession"])) and (f.index.equals(s.index))
    if equal(first_files, second_files):
        return True
    else:
        print("  Fix pairing order")
        row_count = len(second_files.index)
        second_files = second_files.iloc[pd.Index(second_files["File accession"]).get_indexer(first_files["Paired with"])]
        return (equal(first_files, second_files) and len(second_files.index) == row_count)


def submit_jobs (args, metadata):
    count = 0
    for exp_idx, exp_data in metadata.groupby(level=EXP_COLUMNS):
        if args.number and count >= args.number:
            break
        print("\n\n\nProcess experiment:\n\n  Experiment accession -", exp_idx[0],"\n  Biological replicate -", exp_idx[1])
        first_files = exp_data.loc[exp_idx+(1,)]
        second_files = exp_data.loc[exp_idx+(2,)]
        count = count + 1
        try:
            if not properly_paired(first_files, second_files):
                raise Exception("Failed to fix pairing order")
            print("\n  First:\n", first_files[["File accession", "Paired with"]])
            print("\n  Second:\n", second_files[["File accession", "Paired with"]])
            run_id = "_".join([str(i) for i in exp_idx])
            first_combined = download_files(first_files, run_id+"_R1")
            second_combined = download_files(second_files, run_id+"_R2")
            job_template = {
                "job": {
                    "fastq_file_1": {
                        "class": "File",
                        "location": first_combined
                    },
                    "fastq_file_2": {
                        "class": "File",
                        "location": second_combined
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
        except Exception as err:
            print("\n  Failed to submit job:", err)


def main(argsl=None):
    if argsl is None:
        argsl = sys.argv[1:]
    args,_ = arg_parser().parse_known_args(argsl)
    args = normalize_args(args, ["dag", "number"])

    metadata = get_metadata(args.metadata)
    submit_jobs(args, metadata)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))