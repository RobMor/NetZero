# NetZero Data Collection Tool

This tool is meant to make collecting data from various sources related to 
energy usage much easier. The tool provides a simple command line interface
which orchestrates the entire process from collection to output.

## Installation

First, ensure that you have both Python 3 and pip installed:

```console
$ python --version
$ pip --version
```

Then, all you have to do is run this command to install `netzero` and its dependencies:

```console
$ pip install netzero
```

If you prefer to download the repository yourself you can run the following command
from within the repository to install `netzero` and its dependencies.

```console
$ pip install .
```

These commands will install all dependencies and install `netzero` on your
machine.

In order to actually use the tool you'll need to set up an `config.ini` file.
A template with instructions is included in the file `example_config.ini`.

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
First you need to ensure that your configuration file is set up. You can place
the configuration anywhere you want as along as you pass it in on the commandline.
In this example we will call it `config.ini`. Also, since we don't already have 
a database for our data, we will tell `netzero` to create one called `netzero.db`. 
To collect our data we can run this command:

```console
$ netzero collect +psw -s 2019-08-01 -c config.ini -d netzero.db
```

Lets break this command down:

* `netzero collect`: tells netzero that we are **collecting** data with this command.

* `+psw`: adds Pepco (`p`), Solar (`s`) and Weather (`w`) data to the list of
sources to collect from (You can get a full list of sources from `netzero collect -h`).

* `-s 2019-08-01`: tells `netzero` to only collect data after August 1st 2019.
The date must follow the format `YYYY-MM-DD`.

* `-c config.ini`: specifies the file name of our configuration file.

* `-d netzero.db`: gives the filename of our database.

For more options you can check out the help information using `netzero -h`.

## TODO

* Timezones
* Deal with missing GSHP data
