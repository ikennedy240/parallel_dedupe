# loads the full data
library(data.table)
library(glue)
args <- commandArgs(trailingOnly=TRUE)
in_path <- args[1]

df <- fread(in_path, nrows = n_rows)
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
  slurm_lines <- glue('Rscript parallel_dedupe.R {cbsa_in_path} {cbsa_out_path} .7 >> dedupe_log_{focal_cbsa}.txt\n\nexit 0')
  write_lines(slurm_lines, slurm_path, append = TRUE)
  # submits it to the slurm queue
  system(glue('sbatch {slurm_path}'))
}