#!/bin/bash

#$ -N stagein_inhale
#$ -cwd

# Choose the staging environment
#$ -q staging

# Set runtime limit
#$ -l h_rt=12:00:00

# Set the job to resubmit itself if it runs out of time
#$ -r yes
#$ -notify

trap 'exit 99' sigusr1 sigusr2 sigterm

# Source path on DataStore
SOURCE="/exports/csce/datastore/inf/groups/specknet/Speckled Students DataStore/Causal Discovery/DAPHNE/data"

# -p creates directory only if it does not already exist
mkdir -p /exports/eddie/scratch/s2031209/DAPHNE/

# Destination path on Eddie
DESTINATION="/exports/eddie/scratch/s2031209/DAPHNE/"

# Perform copy with rsync
rsync -rl "$SOURCE" "$DESTINATION"