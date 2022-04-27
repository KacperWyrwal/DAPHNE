#!/bin/bash
#$ -N daphne_analysis
#$ -cwd

#$ -pe sharedmem 4
#$ -l h_vmem=2G

. /etc/profile.d/modules.sh
module load anaconda
source activate causal

path_to="/exports/eddie/scratch/s2031209/DAPHNE/results/"
path_from="/exports/eddie/scratch/s2031209/DAPHNE/data/"

echo "Starting analysis: ID = $1, test = $2, max tau = $3, fraction of data used = $4, per_trial = $5"
python3 analyse_per_trial.py "$1" -test "$2" -tau "$3" -ratio "$4" -per_trial "$5" -path_from "$path_from" -path_to "$path_to"

