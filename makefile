# short commit hash
GIT_COMMIT=`git rev-parse --short HEAD`

.DEFAULT_GOAL := help

VERSION_FILE:=bricklayer/__version__.py
VERSION:=$(shell cut -d \' -f 2 ${VERSION_FILE})

help:
	@echo "Build targets:"
	@echo "- clean:                     cleans the build directory"
	@echo "- build_wheel:          		builds wheel package locally"

clean:
	rm -rf build/*; rm -rf dist/*

build_wheel:
	python setup.py sdist bdist_wheel
