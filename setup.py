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
        'matplotlib',
        'pandas',
        'pytz==2018.7',
        # 'numpy==1.22.3',
        'seaborn==0.11.1',
        'statsmodels',
        'imageio==2.9.0',
        'numba',
        'ta==0.7.0',
        'requests==2.25.1',
        'aiohttp==3.7.4.post0',
        'scipy',
        'enum34==1.1.10',
        'dataclasses==0.6',
        'yfinance==0.1.59',
        'colorama==0.3.9',
        'python-dateutil==2.8.1',
        'python-dotenv',
    ],
    packages=find_packages()
)
