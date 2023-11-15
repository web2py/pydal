.PHONY: venv test build deploy

venv:
	python -m venv venv
test: venv
	venv/bin/python -m unittest tests
build: venv
	rm -rf dist/*
	venv/bin/pip install --upgrade build
	venv/bin/pip install --upgrade twine
	venv/bin/python -m build
deploy: build
	venv/bin/python -m twine upload dist/*
