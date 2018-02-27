import os
from setuptools import setup,find_packages
here = os.path.abspath(os.path.dirname(__file__))

install_requires = [
    'requests',
    'mwparserfromhell',
    'pandas',
    'joblib',
    'beautifulsoup4',
    'json',
    'geopy'
    ]

setup(
	name='johnny5',
	version = '0.1',
	author = "C. Jara-Figueroa",
    author_email = "crisjf@mit.edu",
    description = ("Tools for getting data on historical characters from Wikipedia and Wikidata."),
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite="nose.collector"
)
#python setup.py develop