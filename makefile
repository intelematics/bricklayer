# short commit hash
GIT_COMMIT=`git rev-parse --short HEAD`

.DEFAULT_GOAL := help

VERSION_FILE:=VERSION
SEMVER_MAJOR:=$(shell cut -d . -f 1 ${VERSION_FILE})
SEMVER_MINOR:=$(shell cut -d . -f 2 ${VERSION_FILE})
SEMVER_PATCH:=$(shell cut -d . -f 3 ${VERSION_FILE})
VERSION:=${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}
TAG:=bricklayer:${VERSION}
export TAG=$TAG

help:
	@echo "Build targets:"
	@echo "- clean:                     cleans the build directory"
	@echo "- build_wheel:          		builds wheel package locally"

clean:
	rm -rf build/*; rm -rf dist/*

build_wheel:
	python setup.py sdist bdist_wheel

