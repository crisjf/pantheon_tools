# run this script to build the html
cp conf.py rst/conf.py
make html
sphinx-build -b html rst/ html/