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
df <- fread(in_path, nrows = n_rows, colClasses = 'character')
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

recursive_dedupe <- function(df, thresh, cutoff = 500, tmp_file = 'tmp/tmp.csv'){
  # as long as the df is bigger than cutoff, process in chunks
  while(nrow(df)>cutoff){
    print(glue('RUNNING RECURSIVELY: DF HAS {nrow(df)} ROWS AND CUTOFF IS {cutoff}'))
    # grab the first -cutoff- rows of df
    to_run <- head(df, cutoff)
    # run the dedupe on that
    short_df <- recursive_dedupe(to_run, thresh)
    print(glue("SHORT DF SIZE IS {nrow(short_df)}"))
    # if the deduped one is close to the cutoff, save it for now
    if(nrow(short_df)>=(cutoff*.9)){
      print("SHORT DF IS BLOATED, WRITING TO FILE")
      fwrite(short_df, tmp_file, append = TRUE)
      # then df just loses cutoff rows
      df <- df[cutoff:nrow(df)]
    } else { #otherwise cutoff the top of df and add short_df on
      df <- rbind(short_df, df[cutoff:nrow(df)])
    }
  }
  texts <- df$dupeText
  results<- mclapply(1:length(texts), jaccard_parallel, texts = texts, thresh = thresh)
  keep_list <- mclapply(1:length(results), deduped_list, results = results)
  keep_list <- unlist(keep_list)
  df <- df[keep_list]
  # if we had saved a short df of deduped stuff, now bind it back on
  if(exists('short_df') & file.exists(tmp_file)){
    print("RELOADING SHORT DF TO RETURN")
    df <- rbind(df, fread(tmp_file), colClasses = 'caracter')
    file.remove(tmp_file)
  }
  return(df)
}

cbsa <- unique(df$cbsa)
cat(glue('\n\nRUNNING ADDRESS DEDPULICATION ON {nrow(df)} rows from {cbsa}\n'))
deduped_df <- data.table()
addresses <- df[, .N, by=.(geo_address)]
addresses <- addresses[N>10 & !is.na(geo_address) & geo_address!='']$geo_address

# make the cbsa tmp directory if it doesn't exist, otherwise make sure it's cleaned out
if(!dir.exists(glue('tmp/{cbsa}'))){
  dir.create(glue('tmp/{cbsa}'))
} else {
  file.remove(glue('tmp/{cbsa}/*'))
}


for(focal_address in addresses){
  addr_df <- df[geo_address == focal_address]
  cat(glue('\n\nRUNNING DEDPULICATION ON {nrow(addr_df)} rows from {focal_address}\n\n'))
  to_add <- recursive_dedupe(addr_df, thresh, tmp_file = glue('tmp/{cbsa}/{focal_address}.csv'))
  deduped_df <- rbind(deduped_df, to_add)
}

# drop the addresses we've already processed to slim down df
df <- df[!(geo_address %in% addresses)]
cat(glue('\n\nRUNNING TRACT DEDPULICATION ON {nrow(df)} rows from {cbsa}\n'))
tracts <- df[, .N, by=.(geoid)]
tracts <- tracts[N>10 & !is.na(geoid)]$geoid
for(i in 1:length(tracts)){
  focal_tract <- tracts[i]
  tracts_df <- df[geoid == focal_tract]
  cat(glue('\n\nRUNNING DEDPULICATION ON {nrow(tracts_df)} rows from {focal_tract}\n\n'))
  to_add <- recursive_dedupe(tracts_df, thresh)
  deduped_df <- rbind(deduped_df, to_add)
}

df <- df[!(geoid %in% tracts)]
cat(glue('\n\nRUNNING FINAL DEDPULICATION ON {nrow(df)} rows from {cbsa}\n'))
to_add <- recursive_dedupe(df, thresh)
deduped_df <- rbind(deduped_df, to_add)

cat(glue("\nProcessing complete, sucessfully dropped {orig_rows - nrow(deduped_df)} rows\n\n WRITING FILE"))

fwrite(deduped_df, out_path)
cat("\n\nFILE WRITE COMPLETE\n")
