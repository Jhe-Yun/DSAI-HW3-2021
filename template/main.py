
# You should not modify this part.
def config():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--consumption", default="training_data.csv", help="input the consumption data path")
    parser.add_argument("--generation", default="testing_data.csv", help="input the generation data path")
    parser.add_argument("--bidresult", default="output.csv", help="input the bids result path")
    parser.add_argument("--output", default="output.csv", help="output the bids path")

    return parser.parse_args()


if __name__ == "__main__":
    args = config()

    with open(args.output, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["test", 1, 2, 3])
