#!/bin/bash

## Job Name
#SBATCH --job-name=dedupe_cl

## Partition and Allocation
#SBATCH -p stf
#SBATCH -A stf

## Resources
#SBATCH --nodes=1
#SBATCH --time=48:00:00
#SBATCH --ntasks=27
#SBATCH --mem=100G

## specify the working directory for this job
#SBATCH --chdir=.

## Import any modules here
module load r_3.6.0

## scripts to be executed here
