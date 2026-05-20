.PHONY: uv ruff typecheck test lock build deploy

uv:
	which uv || curl -LsSf https://astral.sh/uv/install.sh | sh
check: uv
	uv tool run ruff check
format: uv
	uv tool run ruff format
# Static type-check the new refactor surface (AST, translator, compilers,
# driver). The rest of pydal uses metaclasses and dynamic attributes that
# mypy can't follow, so we scope the check to the modules that should
# stay clean.
typecheck: uv
	uv tool run mypy --follow-imports=silent --ignore-missing-imports \
	    pydal/ast.py pydal/ast_translate.py pydal/driver.py \
	    pydal/compilers/__init__.py pydal/compilers/sql.py \
	    pydal/compilers/sqlite.py
test: check typecheck
	uv run --extra test -m unittest tests
build: test
	rm -rf dist/* build/*
	uv build
publish: build
	uv run --extra manage python -m twine upload dist/*
