# Miraldi Lab Workflow Job Generator


## get-salmon-deseq-job

Generates job file for [salmon-deseq.cwl](https://github.com/MiraldiLab/workflows/blob/master/workflows/salmon-deseq.cwl)

**Don't forget:**

  `-m`, `--metadata` parameter should point to the TSV file with at least two columns:
  `file` and `category`. Column `file` is used as an index so it should always
  be the first.