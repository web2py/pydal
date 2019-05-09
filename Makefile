.PHONY: clean build install deploy test.sql

clean:
	rm dist/* || echo ''
	python3 setup.py clean
build: clean
	python3 setup.py build
install: build
	python3 setup.py install
test.sql: install
	python3 -m unittest tests.sql
deploy: build
	#http://guide.python-distribute.org/creation.html
	python3 setup.py sdist
	twine upload dist/*