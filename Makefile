.PHONY: test-venv test build deploy

test-venv:
	python -m venv test-venv
	test-venv/bin/pip install -U pip
	test-venv/bin/pip install -r test-requirements.txt
test: test-venv
	test-venv/bin/python -m unittest tests
build:
	rm -rf dist/*
	python -m build
deploy: build
	python -m twine upload dist/*
