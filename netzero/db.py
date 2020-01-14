import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base


ModelBase = declarative_base()


def add_args(parser):
    parser.add_argument("-d",
                        required=True,
                        metavar="database",
                        help="stores data in the specified database",
                        dest="database")


# TODO name sucks
def main(arguments):
    engine = sqlalchemy.create_engine("sqlite:///" + arguments.database)

    ModelBase.metadata.create_all(engine)

    return engine