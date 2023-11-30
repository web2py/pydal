.PHONY: test build deploy

test:
	python -m unittest tests
build:
	rm -rf dist/*
	python -m build
deploy: build
	python -m twine upload dist/*
