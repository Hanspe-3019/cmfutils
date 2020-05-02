''' setup for pip install
'''
from setuptools import setup

setup(
    name='cmfutils',
    version='0.3',
    packages=['cmfutils'],
    python_requires='>=3.8',
    install_requires=[
        'pandas >= 1.0',
        ],
)
