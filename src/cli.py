import argparse


def get_arguments():
    parser = argparse.ArgumentParser(
        "Bitcoin Graph",
        description="Bitcoin Graph is an ongoing open-source initiative designed to help users analyze and understand the Bitcoin blockchain using graph theory, machine learning, and statistical physics. The project provides tools to model the blockchain as a dynamic graph, detect signals related to major cryptocurrency events, and compare Bitcoin price movements to established stochastic models.",
    )

    parser.add_argument(
        "year_start",
        type=int,
        help="Year to begin the data processing, must be >= 2009.",
    )
    parser.add_argument(
        "year_end",
        type=int,
        help="Year to end the data processing, must be >= year_start and <= 2021.",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--C",
        action="store_true",
        help="Process the data from ../data/ and save to CSVs.",
    )
    group.add_argument(
        "--P",
        action="store_true",
        help="Make the plots from the data in the CSVs and saves them.",
    )
    group.add_argument(
        "--B",
        action="store_true",
        help="Process the data from ../data/ and save to CSVs, then make the plots from the data in the CSVs and saves them.",
    )

    args = parser.parse_args()

    return args
