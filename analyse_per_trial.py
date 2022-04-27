import argparse
import os
from pcmci_pipeline_per_trial import run_pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("ID", type=str)
    parser.add_argument("-test", type=str, default='ParCorr')
    parser.add_argument("-tau", type=int, default=30)
    parser.add_argument("-ratio", type=float, default=1.0)
    parser.add_argument("-per_trial", type=int, default=1)
    parser.add_argument("-path_to", type=str)
    parser.add_argument("-path_from", type=str)
    args = parser.parse_args()

    path_to = os.path.join(args.path_to, f"{args.tau}_min")

    print(f"It do be running with ID={args.ID}, test={args.test}, tau={args.tau}, ratio={args.ratio}, "
          f"per_trial={args.per_trial}")
    run_pipeline(ID=args.ID, path_from=args.path_from, path_to=path_to, cond_ind_test=args.test, tau_max=args.tau,
                 data_length_ratio=args.ratio, per_trial=args.per_trial)
