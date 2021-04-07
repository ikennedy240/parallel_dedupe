# loads the full data
library(data.table)
library(glue)
args <- commandArgs(trailingOnly=TRUE)
in_path <- args[1]

df <- fread(in_path)
cbsas <- unique(df$cbsafp)
# loops through CBSAs
for(focal_cbsa in cbsas){
  cbsa_in_path <- glue('raw_data/raw_{focal_cbsa}.csv.gz')
  cbsa_out_path <- glue('deduped_data/deduped_{focal_cbsa}.csv.gz')
  slurm_path <- glue('submit_{focal_cbsa}.slurm')
  if(file.exists(cbsa_out_path)){
    cat(glue("Outfile exists for {focal_cbsa}, continuing to next group"))
    next
  }
  if(!file.exists(cbsa_in_path)){
    # pulls out the current cbsa
    cbsa_df <- df[cbsa == focal_cbsa]
    # writes to a file
    fwrite(cbsa_df, cbsa_in_path)
  }
  # reads in the base_submit.slurm
  file.copy('base_submit.slurm', slurm_path, overwrite = TRUE)
  # modifies it for the current CBSA
  job_name <- glue('cl_dedupe_{focal_cbsa}')
  slurm_lines <- glue('#!/bin/bash
  
## Job Name
#SBATCH --job-name={job_name}

## Partition and Allocation
#SBATCH -p stf\n#SBATCH -A stf

## Resources
#SBATCH --nodes=1
#SBATCH --time=48:00:00
#SBATCH --ntasks=27
#SBATCH --mem=100G

## specify the working directory for this job
#SBATCH --chdir=.

## Import any modules here
module load r_3.6.0

## scripts to be executed hereRscript parallel_dedupe.R {cbsa_in_path} {cbsa_out_path} .7 >> dedupe_log_{focal_cbsa}.txt

exit 0')
  write(slurm_lines, slurm_path, append = FALSE)
  # submits it to the slurm queue
  system(glue('sbatch {slurm_path}'))
}
