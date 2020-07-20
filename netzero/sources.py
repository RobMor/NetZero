import entrypoints


def load():
    for name, entrypoint in entrypoints.get_group_named("netzero.sources").items():
        source = entrypoint.load()
        yield name, source


def add_args(parser):
    sources_group = parser.add_argument_group(
        "data sources", description="Flags used to select the supported data sources"
    )

    for name, source in load():
        if not hasattr(source, "name"):
            source.name = name
        if not hasattr(source, "option"):
            source.option = source.name[0]
        if not hasattr(source, "long_option"):
            source.long_option = source.name

        sources_group.add_argument(
            "+" + source.option,  # Shorthand argument
            "--" + source.long_option,  # Longform argument
            help=source.summary,
            dest="sources",
            action="append_const",
            const=source,
        )
