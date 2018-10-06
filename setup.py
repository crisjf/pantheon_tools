import setuptools

with open ('README.md','r') as fh:
    long_description = fh.read()

setuptools.setup(
	name='johnny5',
	version = '0.1',
	author = "Cristian Jara-Figueroa",
    author_email = "crisjf@mit.edu",
    description = "Tools for getting data on historical characters from Wikipedia and Wikidata.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url = "https://crisjf.github.io/johnny5/",
    packages=setuptools.find_packages(),
    classifiers=[
    "Programming Language :: Python :: 2.7",
    "License :: OSI Approved :: MIT License"]
)

#python setup.py develop