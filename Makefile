.PHONY: uv ruff test lock build deploy

uv:
	which uv || curl -LsSf https://astral.sh/uv/install.sh | sh
check: uv
	uv tool run ruff check
format: uv
	uv tool run ruff format
test: check
	uv run -m unittest tests
build: test
	rm -rf dist/* build/*
	uv build
deploy: build
	uv run -m twine upload dist/*
