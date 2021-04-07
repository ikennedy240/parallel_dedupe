suppressWarnings(library(parallel))
suppressWarnings(library(glue, lib.loc = '/gscratch/stf/ikennedy/rpackages'))
suppressWarnings(library(stringdist, lib.loc = '/gscratch/stf/ikennedy/rpackages'))
suppressWarnings(library(data.table, lib.loc = '/gscratch/stf/ikennedy/rpackages'))
suppressWarnings(library(glue, lib.loc = '/gscratch/stf/ikennedy/rpackages'))

args <-  commandArgs(trailingOnly=TRUE)
if(length(args)<2){
  stop("You have to supply at least an input and output file path")
}
in_path <- args[1]
n_rows <- as.numeric(args[2])
n_cores <- detectCores()
thresh <- .7

print(glue("RUNNING BENCHMARK WITH {n_cores} CORES AT {Sys.time()}"))

print(glue("DATA READ TEST WITH {n_rows} ROWS"))

system.time(df <- fread(in_path, nrows = n_rows))

texts <- df$dupeText

print(glue("SINGLE ROW TEST WITH {length(texts)} TEXTS"))
system.time(stringdistmatrix(a = texts[1],b = texts, method = 'jaccard', q = 10))

print(glue("{length(texts)} ROW TEST WITH {length(texts)} TEXTS"))

jaccard_parallel <- function(i, texts, thresh){
  if(i %% 500 == 0){
    print(glue("Processed {i} rows"))
  }
  # gets the dupes
  dist <- stringdistmatrix(a = texts[i],b = texts, method = 'jaccard', q = 10)
  # returns the list of dupes
  return(which(dist<=thresh))
}

system.time(mclapply(1:length(texts), jaccard_parallel, texts = texts, thresh = thresh))


print(glue("BENCHMARK COMPLETE AT {Sys.time()}"))
