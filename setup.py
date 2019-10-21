#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-campaignmonitor",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_campaignmonitor"],
    install_requires=[
        "singer-python==5.6.1",
        "requests",
        "createsend==5.0.0",
        "pendulum==1.2.0",
        "urllib3==1.24.2",
    ],
    entry_points="""
    [console_scripts]
    tap-campaignmonitor=tap_campaignmonitor:main
    """,
    packages=["tap_campaignmonitor"],
    package_data = {
        "schemas": ["tap_campaignmonitor/schemas/*.json"]
    },
    include_package_data=True,
)
