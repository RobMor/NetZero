import os.path
import configparser

import netzero.dirs

def add_args(parser):
    parser.add_argument(
        "-c",
        required=False,
        metavar="config",
        default=os.path.join(netzero.dirs.user_config_dir("netzero"), "config.ini"),
        help="loads inputs from the specified INI file instead of the default",
        dest="config",
    )

def load_config(path):
    # TODO configuration file generation
    if not os.path.exists(path):
        raise ValueError("Configuration file not found: {}".format(path))

    config = configparser.ConfigParser()

    with open(path) as f:
        config.read_file(f)

    return config