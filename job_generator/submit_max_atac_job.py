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
INDEX_COLUMNS_SRA = ["exp_id", "technical_rep"]
EXP_COLUMNS_SRA = ["exp_id"]
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
    general_parser.add_argument("-f", "--fdump",       help="Path to fastq-dump (use it with --sra). Run from Docker if not set")
    general_parser.add_argument("-s", "--sra",         help="Use metadata file with SRA identifiers",        action="store_true")
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


def get_metadata_sra(metadata_file):
    raw_data = pd.read_table(metadata_file, index_col=INDEX_COLUMNS_SRA, comment='#')
    return raw_data.sort_index()


def extract_sra(srr_files, run_id, fdump=None):
    first_filename = run_id + "_R1.fastq.gz"
    second_filename = run_id + "_R2.fastq.gz"
    first_combined_filepath = os.path.join(CWD, first_filename)
    second_combined_filepath = os.path.join(CWD, second_filename)

    if os.path.isfile(first_combined_filepath) or os.path.isfile(second_combined_filepath):
        raise Exception(f"""File {first_combined_filepath} or {second_combined_filepath} already exists""")

    for srr_id in srr_files:
        if fdump:
            params = f"""wget -q --show-progress https://ftp.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/SRR/{srr_id[0:6]}/{srr_id}/{srr_id}.sra
                        {fdump} --split-3 --gzip {srr_id}.sra &&
                        cat {srr_id}_1.fastq.gz >> {first_filename} &&
                        cat {srr_id}_2.fastq.gz >> {second_filename} &&
                        rm {srr_id}_1.fastq.gz {srr_id}_2.fastq.gz"""
        else:
            params = f"""wget -q --show-progress https://ftp.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/SRR/{srr_id[0:6]}/{srr_id}/{srr_id}.sra
                        docker run --rm -ti -v {CWD}:/tmp/ biowardrobe2/sratoolkit:v2.8.2-1 fastq-dump --split-3 --gzip {srr_id}.sra &&
                        cat {srr_id}_1.fastq.gz >> {first_filename} &&
                        cat {srr_id}_2.fastq.gz >> {second_filename} &&
                        rm {srr_id}_1.fastq.gz {srr_id}_2.fastq.gz"""

        env = os.environ.copy()
        print("\n  Run", params)
        try:
            subprocess.run(params, env=env, shell=True, check=True)
        except Exception as err:
            try:
                os.remove(first_combined_filepath)
                os.remove(second_combined_filepath)
                os.remove(os.path.join(CWD, f"""{srr_id}_1.fastq.gz"""))
                os.remove(os.path.join(CWD, f"""{srr_id}_2.fastq.gz"""))
            except Exception as err:
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
            first_combined, second_combined = extract_sra(srr_files, run_id, args.fdump)
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
    args = normalize_args(args, ["dag", "number", "sra"])
    if args.sra:
        metadata = get_metadata_sra(args.metadata)
        submit_jobs_sra(args, metadata)
    else:
        metadata = get_metadata(args.metadata)
        submit_jobs(args, metadata)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))