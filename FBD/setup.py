#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 17:51:50 2023

@author: javiera.quiroz
"""

from setuptools import setup

setup(
    name='FlyBaseDownloads',
    version='3.1.1',
    license='MIT',
    author='Javiera Quiroz Olave',
    url='https://github.com/JavieraQuirozO/FBD',
    author_email='javiera.quiroz@biomedica.udec.cl',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[req.strip() for req in open('requirements.txt').readlines()],
    description='Wrapped to download FlyBase data with Python, easily and quickly.',
    packages=['FlyBaseDownloads', 'FlyBaseDownloads.classes', 'FlyBaseDownloads.downloads', 'FlyBaseDownloads.utilities'],
    include_package_data=True,
    package_data={
        'FlyBaseDownloads': ['.env'], 
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Healthcare Industry',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    python_requires='>=3.6',
)

