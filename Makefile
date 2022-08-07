.PHONY: clean build install deploy test.sql

venv:
	python3 -m venv venv
	# venv/bin/pip install -r requirements.txt
clean:
	rm -f dist/*
	venv/bin/python setup.py clean
build: clean
	venv/bin/python setup.py build
install: build
	venv/bin/python setup.py install
test.sql: install
	venv/bin/python -m unittest tests.sql
deploy: build
	# http://guide.python-distribute.org/creation.html
	venv/bin/python setup.py sdist
	twine upload dist/*
