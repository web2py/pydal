.PHONY: venv test build deploy

venv:
	python -m venv venv
test: venv
	venv/bin/python -m unittest tests.sql
build:
	python -m pip install --upgrade build
	python -m pip install --upgrade twine
	python -m build
deploy: build
	python -m twine upload dist/*
