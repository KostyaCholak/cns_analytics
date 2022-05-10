#!/usr/bin/env python

import pathlib
from setuptools import setup, find_packages


setup(
    name='cns_analytics',
    version='0.1.0',
    description='CNS Analytics',
    author='Kostya Cholak',
    author_email='kostya.cholak@gmail.com',
    install_requires=[
        'asyncpg==0.23.0',
        'matplotlib==3.4.1',
        'pandas==1.2.4',
        'pytz==2018.7',
        'numpy==1.19.5',
        'seaborn==0.11.1',
        'statsmodels==0.12.2',
        'imageio==2.9.0',
        'numba==0.53.1',
        'ta==0.7.0',
        'requests==2.25.1',
        'aiohttp==3.7.4.post0',
        'scipy==1.6.0',
        'enum34==1.1.10',
        'dataclasses==0.6',
        'yfinance==0.1.59',
        'colorama==0.3.9',
        'python-dateutil==2.8.1',
    ],
    packages=find_packages(include=['cns_analytics'])
)
