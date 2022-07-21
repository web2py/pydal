.PHONY: clean build install deploy test.sql

clean:
	rm dist/* || echo ''
	python setup.py clean
build: clean
	python setup.py build
install: build
	python setup.py install
test.sql: install
	python -m unittest tests.sql
deploy: build
	#http://guide.python-distribute.org/creation.html
	python setup.py sdist
	twine upload dist/*
