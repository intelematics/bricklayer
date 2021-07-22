# short commit hash
GIT_COMMIT=`git rev-parse --short HEAD`

.DEFAULT_GOAL := help

help:
	@echo "Build targets:"
	@echo "- clean:                     cleans the build directory"
	@echo "- build_wheel:          		builds wheel package locally"

clean:
	rm -rf build/*; rm -rf dist/*

build_wheel:
	python setup.py sdist bdist_wheel; mkdir -p package; cp dist/*.whl package

