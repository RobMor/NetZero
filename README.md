# NetZero Data Collection Tool

This tool is meant to make collecting data from various sources related to 
energy usage much easier. The tool provides a simple command line interface
which orchestrates the entire process from collection to output.

## Installation

First, ensure that you have pip installed for Python 3:

```console
$ pip --version
pip 19.1.1 from ... (python 3.7)
```

Then, all you have to do is run pip on this repository:

```console
$ pip install .
```

Or you can download and install `netzero` all in one step like so:

```console
$ pip install git+https://github.com/RobMor/NetZero.git
```

These commands will install all dependencies and install `netzero` on your
machine.

In order to actually use the tool you'll need to set up an `options.json` file.
A template with instructions is included in this repository.

## Usage

This tool offers a simple command line interface that can be accessed by running

```console
$ netzero
usage: netzero [-h] <command> ...

Collects and formats data from multiple sources

optional arguments:
  -h, --help  show this help message and exit

available commands:
  <command>
    collect   Collect data
    format    Format data

```

## Example

Say you wanted to collect Pepco, SolarEdge, and Weather data since August 2019.
First you need to ensure that your configuration file is set up. In this example
we will call it `config.json`. Also since we don't already have a database for 
our data, we will tell `netzero` to create one called `netzero.db`. Okay, to 
collect our data we can run this command:

```console
$ netzero collect +psw --start=2019-08-01 -c config.json netzero.db
```

Lets break this command down:

* `netzero collect`: tells netzero that we are **collecting** data with this command.

* `+psw`: adds Pepco (`p`), Solar (`s`) and Weather (`w`) data to the list of
sources to collect from (You can get a full list of sources from `netzero collect -h`).

* `--start=2019-08-01`: tells `netzero` to only collect data after August 1st 2019.
The date must follow the format `YYYY-MM-DD`.

* `-c config.json`: specifies the file name of our configuration file.

* `netzero.db`: gives the filename of our database.

For more options you can check out the help information using `-h`.

## TODO

* Timezones
* Deal with missing GSHP data
