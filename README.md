# NetZero Data Collection Tool

This tool is meant to make collecting data from various sources related to 
energy usage much easier. The tool provides a simple command line interface
which orchestrates the entire process from collection to output.

## Installation

First, ensure that you have Python 3 installed:

```console
$ python --version
Python 3.7.3
```

Then, all you have to do is run the setup.py installation script like so:

```console
$ python setup.py install
```

This command will install all dependencies and install the command on your
machine.

In order to actually use the tool you'll need to set up an `options.json` file.
A template with instructions is included in this repository.

## Usage

This tool offers a simple command line interface that can be accessed by running

```console
$ python -m netzero
<INSERT HELP MESSAGE HERE>
```

## Example

Say you wanted to collect Pepco, SolarEdge, and Weather data. First you

## TODO

* Timezones