"""NetZero Data Collection Tool

"""
from setuptools import setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="netzero",
    version="0.1.0",
    author="Robert Morrison",
    author_email="robbieguy98@gmail.com",
    description="A collection of tools to mine data on the efficiency of a house",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/RobMor/NetZero",
    packages=["netzero", "netzero.builtin"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
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
