import setuptools

with open ('README.md','r') as fh:
    long_description = fh.read()

install_requires = ['requests==2.18.4',
                    'mwparserfromhell==0.5',
                    'pandas==0.22.0',
                    'joblib==0.11',
                    'beautifulsoup4==4.6.0',
                    'nltk==3.4.5',
                    'python-dateutil==2.6.1',
                    'spotipy==2.4.4',
                    'urllib3==1.26.5']

setuptools.setup(
	name='johnny5',
	version = '0.0.5',
	author = "Cristian Jara-Figueroa",
    author_email = "crisjf@mit.edu",
    description = "Tools for getting data on historical characters from Wikipedia and Wikidata.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url = "https://crisjf.github.io/johnny5/",
    install_requires=install_requires,
    packages=setuptools.find_packages(),
    classifiers=[
    "Programming Language :: Python :: 2.7",
    "License :: OSI Approved :: MIT License"]
)

# To setup for development:
#> python setup.py develop

# To setup for deployment in pypi:
#> python setup.py sdist
#> twine upload --repository-url https://test.pypi.org/legacy/ dist/*

# To install from pypi:
#> pip install --index-url https://test.pypi.org/simple/ johnny5==0.0.5
