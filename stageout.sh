#!/bin/bash

#$ -N stageout_inhale
#$ -cwd

# Choose the staging environment
#$ -q staging

# Hard runtime limit
#$ -l h_rt=12:00:00

# Make the job resubmit itself if it runs out of time
#$ -r yes
#$ -notify

#$ -hold_jid inh_analyse_arr

trap 'exit 99' sigusr1 sigusr2 sigterm

# Source path on Eddie
SOURCE="/exports/eddie/scratch/s2031209/DAPHNE/results/"

# TOOD decide where to stage out the results
mkdir -p /exports/csce/datastore/inf/groups/specknet/Speckled Students DataStore/Causal Discovery/DAPHNE/kacper_results
DESTINATION="/exports/csce/datastore/inf/groups/specknet/Speckled Students DataStore/Causal Discovery/DAPHNE/kacper_results/"
rsync -rl "$SOURCE" "$DESTINATION"