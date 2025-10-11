import argparse


def get_arguments():
    parser = argparse.ArgumentParser(
        "Bitcoin Graph",
        description="Bitcoin Graph is an ongoing open-source initiative designed to help users analyze and understand the Bitcoin blockchain using graph theory, machine learning, and statistical physics. The project provides tools to model the blockchain as a dynamic graph, detect signals related to major cryptocurrency events, and compare Bitcoin price movements to established stochastic models.",
    )

    # group_res = parser.add_mutually_exclusive_group(required=True)
    # group_res.add_argument(
    #     "-y", "--year",
    #     type=str,
    #     help="Sets resolution to years.",
    # )
    # group_res.add_argument(
    #     "-h",
    #     type=str,
    #     help="Sets resolution to hours",
    # )
    parser.add_argument(
        "-r",
        "--resolution",
        choices=["hour", "year"],
        default="year",
        metavar="",
        help="Resolution of the snapshots (choices: %(choices)s) [default: %(default)s]",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-c",
        "--compute",
        action="store_true",
        help="Process the data from ../data/ and save to CSVs.",
    )
    group.add_argument(
        "-p",
        "--plot",
        action="store_true",
        help="Make the plots from the CSVs in ../data/ and saves them in ../plots/.",
    )
    group.add_argument(
        "-b",
        "--both",
        action="store_true",
        help="Process the data from ../data/ and save to CSVs, then make the plots from the data in the CSVs and saves them.",
    )

    args = parser.parse_args()

    return args
