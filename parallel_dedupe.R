#!/usr/bin/env Rscript
suppressWarnings(library(parallel))
suppressWarnings(library(glue, lib.loc = '/gscratch/stf/ikennedy/rpackages'))
suppressWarnings(library(stringdist, lib.loc = '/gscratch/stf/ikennedy/rpackages'))
suppressWarnings(library(data.table, lib.loc = '/gscratch/stf/ikennedy/rpackages'))
suppressWarnings(library(glue, lib.loc = '/gscratch/stf/ikennedy/rpackages'))
## laod data
args <-  commandArgs(trailingOnly=TRUE)
if(length(args)<2){
  stop("You have to supply at least an input and output file path")
}
in_path <- args[1]
out_path <- args[2]

if(length(args)>2){
  thresh <- as.numeric(args[3])
} else {
  thresh <- .7
}

if(length(args)>3){
  n_rows <- as.numeric(args[4])
} else {
  n_rows <- Inf
}

cat(glue("Reading file from {in_path}\nWriting file to {out_path}\nUsing threshold:{thresh} and reading {n_rows} rows\n"))

cat('\n\nREADING DATA\n')
df <- fread(in_path, nrows = n_rows)
orig_rows <- nrow(df)
## function that calcs jaccard with all
jaccard_parallel <- function(i, texts, thresh){
  # gets the dupes
  dist <- stringdistmatrix(a = texts[i],b = texts, method = 'jaccard', q = 10)
  # returns the list of dupes
  return(which(dist<=thresh))
}

## function that drops the dupes based on resuts from lapply jaccard_parallel

deduped_list <- function(i, results){
  if(i!=min(results[[i]])){
    return(FALSE)
  } 
  if(i!=min(unlist(results[sapply(results, function(y) i %in% y)]))){
    return(FALSE)
  }
  return(TRUE)
}
cat(glue('\n\nRUNNING ADDRESS DEDPULICATION ON {nrow(df)} rows from {cbsa}\n'))
deduped_df <- data.table()
addresses <- df[, .N, by=.(geo_address)]
addresses <- addresses[N>10 & !is.na(geo_address)]$geo_address

for(focal_address in addresses){
  addr_df <- df[geo_address == focal_address]
  cat(glue('\n\nRUNNING DEDPULICATION ON {nrow(addr_df)} rows from {focal_address}\n\n'))
  texts <- addr_df$dupeText
  results <- mclapply(1:length(texts), jaccard_parallel, texts = texts, thresh = thresh)
  keep_list <- mclapply(1:length(results), deduped_list, results = results)
  keep_list <- unlist(keep_list)
  deduped_df <- rbind(deduped_df, addr_df[keep_list])
}

# drop the addresses we've already processed to slim down df
df <- df[!(geo_address %in% addresses)]
cat(glue('\n\nRUNNING FULL DEDPULICATION ON {nrow(df)} rows from {cbsa}\n'))
texts <- df$dupeText
results <- mclapply(1:length(texts), jaccard_parallel, texts = texts, thresh = thresh)
cat('\n\nPROCESSING RESULTS\n')
keep_list <- sapply(1:length(results), deduped_list, results = results)
deduped_df <- rbind(deduped_df, df[keep_list])

cat(glue("\nProcessing complete, sucessfully dropped {orig_rows - nrow(deduped_df)} rows\n\n WRITING FILE"))

fwrite(deduped_df, out_path)
cat("\n\nFILE WRITE COMPLETE\n")
