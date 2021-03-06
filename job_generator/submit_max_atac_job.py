import sys
import os
import argparse
import gzip
import pandas as pd
import subprocess
from json import dumps
from job_generator.utils.utils import normalize_args


INDEX_COLUMNS = ["Experiment accession", "Biological replicate(s)", "Paired end", "Technical replicate(s)"]
EXP_COLUMNS = ["Experiment accession", "Biological replicate(s)"]
FILTER_COLUMN = "Run type"
FILTER_VALUE = "paired-ended"
INDEX_COLUMNS_SRA = ["exp_id", "technical_rep"]
EXP_COLUMNS_SRA = ["exp_id"]
CWD = os.getcwd()


def arg_parser():
    general_parser = argparse.ArgumentParser()
    general_parser.add_argument("-m", "--metadata",    help="Path to metadata file",                         required=True)
    general_parser.add_argument("-d", "--dag",         help="Dag id",                                        required=True)
    general_parser.add_argument("-i", "--indices",     help="Path to indices folder",                        required=True)
    general_parser.add_argument("-b", "--blacklisted", help="Path to blacklisted regions file",              required=True)
    general_parser.add_argument("-g", "--genome",      help="Path to genome FASTA file",                     required=True)
    general_parser.add_argument("-n", "--number",      help="Limit number of experiments to submit",         type=int)
    general_parser.add_argument("-c", "--counts",      help="Limit read counts per submitted experiment",    type=int, default=100000000)
    general_parser.add_argument("-t", "--threads",     help="Threads number",                                type=int, default=4)
    general_parser.add_argument("-o", "--output",      help="Path to be used as output_folder in job files", required=True)
    general_parser.add_argument("-f", "--fdump",       help="Path to fastq-dump (use it with --sra). Run from Docker if not set")
    general_parser.add_argument("-s", "--sra",         help="Use metadata file with SRA identifiers",        action="store_true")
    general_parser.add_argument("-l", "--local",       help="Work with pre-downloaded fastq files. Only for --sra", action="store_true")
    general_parser.add_argument("-r", "--rerun",       help="Rerun expreriment",                             action="store_true")
    general_parser.add_argument("-w", "--download",    help="Skip triggering. Only download data",           action="store_true")
    general_parser.add_argument("-p", "--suffix",      help="Run id suffix",                                 type=str, default="")
    return general_parser


def get_metadata(metadata_file):
    raw_data = pd.read_table(metadata_file, index_col=INDEX_COLUMNS, comment='#')
    return raw_data.loc[raw_data[FILTER_COLUMN] == FILTER_VALUE].sort_index()


def get_reads_count(filename):
    counter = 0
    with gzip.open(filename, "rb") as input_stream:
        for counter, _ in enumerate(input_stream):
            pass
    return int((counter + 1) / 4)


def download_files(files_df, prefix, rerun, max_counts):
    combined_filepath = os.path.join(CWD, prefix + ".fastq.gz")
    if rerun:
        if os.path.isfile(combined_filepath):
            return combined_filepath
        else:
            raise Exception(f"""File {combined_filepath} is missing. Cannot rerun""")
    else:
        if os.path.isfile(combined_filepath):
            raise Exception(f"""File {combined_filepath} already exists""")
    current_counts = 0
    remaining_counts = max_counts
    for file_url in files_df["File download URL"]:
        if remaining_counts == 0:
            print(f"\n  Skip download. Reached max reads counts ({current_counts})")
            break
        current_filename = file_url.split("/")[-1]
        current_filepath = os.path.join(CWD, current_filename)
        if os.path.isfile(current_filepath):
            params = " ".join(["cat", current_filepath, "| zcat | head -n", str(4*remaining_counts), "| gzip  >>", combined_filepath])
        else:
            params = " ".join(["wget", "-O", "-", "-q", "--show-progress", file_url, "| zcat | head -n", str(4*remaining_counts), "| gzip  >>", combined_filepath])
        env = os.environ.copy()
        print("\n  Run", params)
        try:
            subprocess.run(params, env=env, shell=True, check=True)
        except Exception as err:
            os.remove(combined_filepath)
            raise err
        current_counts = get_reads_count(combined_filepath)
        remaining_counts = max_counts - current_counts
    return combined_filepath


def trigger_dag(job, run_id, dag_id, suffix):
    params = ["airflow", "trigger_dag", "-r", run_id+suffix, "-c", dumps(job), dag_id]
    env = os.environ.copy()
    print("Trigger dag", params)
    subprocess.run(params, env=env)


def equal(f,s):
    return (f["Paired with"].equals(s["File accession"])) and (s["Paired with"].equals(f["File accession"])) and (f.index.equals(s.index))


def get_properly_paired_second_files(first_files, second_files):
    if equal(first_files, second_files):
        return second_files
    else:
        print("  Fixed pairing order")
        row_count = len(second_files.index)
        return second_files.iloc[pd.Index(second_files["File accession"]).get_indexer(first_files["Paired with"])]


def get_metadata_sra(metadata_file):
    raw_data = pd.read_table(metadata_file, index_col=INDEX_COLUMNS_SRA, comment='#')
    return raw_data.sort_index()


def extract_sra(srr_files, run_id, fdump, local, rerun):
    first_filename = run_id + "_R1.fastq.gz"
    second_filename = run_id + "_R2.fastq.gz"
    first_combined_filepath = os.path.join(CWD, first_filename)
    second_combined_filepath = os.path.join(CWD, second_filename)

    if rerun:
        if os.path.isfile(first_combined_filepath) and os.path.isfile(second_combined_filepath):
            return first_combined_filepath, second_combined_filepath
        else:
            raise Exception(f"""File {first_combined_filepath} or {second_combined_filepath} is missing. Cannot rerun""")
    else:
        if os.path.isfile(first_combined_filepath) or os.path.isfile(second_combined_filepath):
            raise Exception(f"""File {first_combined_filepath} or {second_combined_filepath} already exists""")

    for srr_id in srr_files:
        if fdump:
            download = f"""{fdump} --split-3 --gzip {srr_id} &&"""
            combine = f"""cat {srr_id}_1.fastq.gz >> {first_filename} &&
                        cat {srr_id}_2.fastq.gz >> {second_filename} &&
                        rm {srr_id}_1.fastq.gz {srr_id}_2.fastq.gz"""
        else:
            download = f"""docker run --rm -ti -v {CWD}:/tmp/ biowardrobe2/sratoolkit:v2.8.2-1 fastq-dump --split-3 --gzip {srr_id} &&"""
            combine = f"""cat {srr_id}_1.fastq.gz >> {first_filename} &&
                        cat {srr_id}_2.fastq.gz >> {second_filename} &&
                        rm {srr_id}_1.fastq.gz {srr_id}_2.fastq.gz"""

        env = os.environ.copy()
        if not local:
            params = download + "\n" + combine
        else:
            params = combine
        print("\n  Run", params)
        try:
            subprocess.run(params, env=env, shell=True, check=True)
        except Exception as err:
            try:
                os.remove(first_combined_filepath)
                os.remove(second_combined_filepath)
                os.remove(os.path.join(CWD, f"""{srr_id}_1.fastq.gz"""))
                os.remove(os.path.join(CWD, f"""{srr_id}_2.fastq.gz"""))
            except Exception:
                pass
            raise err
    return first_combined_filepath, second_combined_filepath


def submit_jobs_sra (args, metadata):
    count = 0
    for exp_idx, exp_data in metadata.groupby(level=EXP_COLUMNS_SRA):
        if args.number and count >= args.number:
            break
        print("\n\n\nProcess experiment:\n\n  Experiment accession -", exp_idx)
        srr_files = exp_data["srr_id"].tolist()
        count = count + 1
        try:
            print("\n  SRR: ", srr_files)
            run_id = exp_idx
            first_combined, second_combined = extract_sra(srr_files, run_id, args.fdump, args.local, args.rerun)
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
                    "genome_fasta_file": {
                        "class": "File",
                        "location": args.genome
                    },
                    "genome_size": "2.7e9",
                    "outputs_folder": os.path.join(args.output, run_id),
                    "threads": args.threads
                }
            }
            print(dumps(job_template, indent = 4))
            if not args.download:
                trigger_dag(job_template, run_id, args.dag, args.suffix)
        except Exception as err:
            print("\n  Failed to submit job:", err)


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
            second_files = get_properly_paired_second_files(first_files, second_files)
            print("\n  First:\n", first_files[["File accession", "Paired with"]])
            print("\n  Second:\n", second_files[["File accession", "Paired with"]])
            run_id = "_".join([str(i) for i in exp_idx])
            first_combined = download_files(first_files, run_id+"_R1", args.rerun, args.counts)
            second_combined = download_files(second_files, run_id+"_R2", args.rerun, args.counts)
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
                    "genome_fasta_file": {
                        "class": "File",
                        "location": args.genome
                    },
                    "genome_size": "2.7e9",
                    "outputs_folder": os.path.join(args.output, run_id),
                    "threads": args.threads
                }
            }
            print(dumps(job_template, indent = 4))
            if not args.download:
                trigger_dag(job_template, run_id, args.dag, args.suffix)
        except Exception as err:
            print("\n  Failed to submit job:", err)


def main(argsl=None):
    if argsl is None:
        argsl = sys.argv[1:]
    args,_ = arg_parser().parse_known_args(argsl)
    args = normalize_args(args, ["dag", "number", "sra", "local", "rerun", "threads", "counts", "download", "suffix"])
    if args.sra:
        metadata = get_metadata_sra(args.metadata)
        submit_jobs_sra(args, metadata)
    else:
        metadata = get_metadata(args.metadata)
        submit_jobs(args, metadata)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))