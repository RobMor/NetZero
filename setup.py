"""NetZero Data Collection Tool

"""
from setuptools import find_packages, setup

dependencies = ["requests", "bs4"]

setup(
    name="netzero",
    version="0.1.0",
    author="Robert Morrison",
    author_email="robbieguy98@gmail.com",
    description=
    "A collection of tools to mine data on the efficiency of a house",
    long_description=__doc__,
    url="https://github.com/RobMor/NetZero",
    packages=["netzero"],
    install_requires=["requests", "bs4", "entrypoints"],
    entry_points={
        "console_scripts": ["netzero=netzero.__main__:main"],
        "netzero.sources": [
            "pepco=netzero.builtin.pepco:Pepco",
            "gshp=netzero.builtin.gshp:Gshp",
            "solar=netzero.builtin.solar:Solar",
            "weather=netzero.builtin.weather:Weather",
        ]},
)
