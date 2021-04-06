#!/usr/bin/env Rscript

suppressWarnings(library(parallel))
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

if(!('status' %in% names(df))){
   df$status <- 'unchecked'
}

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

cbsas <- unique(df[df$status == 'unchecked']$cbsa)
cat(glue("\nDetected {length(cbsas)} processing separately\n\n"))

for(focal_cbsa in cbsas){
  df_cbsa <- df[df$cbsa == focal_cbsa]
  df_cbsa <- df_cbsa[order(listing_date),]
  df <- df[df$cbsa != focal_cbsa]
  cat(glue('\n\nRUNNING DEDPULICATION ON {nrow(df_cbsa)} rows from {focal_cbsa}\n'))
  texts <- df_cbsa$dupeText
  results <- mclapply(1:length(texts), jaccard_parallel, texts = texts, thresh = thresh)
  cat('\n\nPROCESSING RESULTS\n')
  keep_list <- sapply(1:length(results), deduped_list, results = results)
  df <- rbind(df,df_cbsa[keep_list])
  cat(glue("\nProcessing complete, sucessfully dropped {length(keep_list)-sum(keep_list)} rows\n\n WRITING FILE"))
  fwrite(df, out_path)
}

cat("\n\nFILE WRITE COMPLETE\n")
