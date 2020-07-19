import os.path

import netzero.dirs

def add_args(parser):
    parser.add_argument("-d",
                        required=False,
                        metavar="database",
                        default=os.path.join(netzero.dirs.user_data_dir("netzero"), "database.sqlite3"),
                        help="stores data in the specified database instead of the default",
                        dest="database")