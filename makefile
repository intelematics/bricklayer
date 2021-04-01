IMAGE_NAME=dbricks_utils
ECR_ACCOUNT=977887051160
ECR_REGION=ap-southeast-2
ECR_URL=${ECR_ACCOUNT}.dkr.ecr.ap-southeast-2.amazonaws.com
S3_PKG_DEV_PREFIX=s3://intelematics-dac-build-development-artifacts/dbricks_utils
S3_PKG_REL_PREFIX=s3://intelematics-dac-build-release-artifacts/dbricks_utils

# semantic version
SEMVER_MAJOR:=$(shell cut -d \' -f 2 < dbricks_utils/__version__.py | cut -d . -f 1)
SEMVER_MINOR:=$(shell cut -d \' -f 2 < dbricks_utils/__version__.py | cut -d . -f 2)
SEMVER_PATCH:=$(shell cut -d \' -f 2 < dbricks_utils/__version__.py | cut -d . -f 3)

# build artifacts
PACKAGE_WHL=dist/${IMAGE_NAME}-${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}-py3-none-any.whl
PACKAGE_WHL_DEV_PATCH=${S3_PKG_DEV_PREFIX}/${IMAGE_NAME}-${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}-py3-none-any.whl
PACKAGE_WHL_REL_PATCH=${S3_PKG_REL_PREFIX}/${IMAGE_NAME}-${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}-py3-none-any.whl
PACKAGE_WHL_DEV_LATEST=${S3_PKG_DEV_PREFIX}/${IMAGE_NAME}-latest-py3-none-any.whl
PACKAGE_WHL_REL_LATEST=${S3_PKG_REL_PREFIX}/${IMAGE_NAME}-latest-py3-none-any.whl

# short commit hash
GIT_COMMIT=`git rev-parse --short HEAD`

.DEFAULT_GOAL := help

help:
	@echo "Build targets:"
	@echo "- clean:                     cleans the build directory"
	@echo "- build_wheel:          		builds wheel package locally"
	@echo "- publish_wheel_dev:        	publishes wheel package as to ECR build artifacts"
	@echo "- publish_wheel_release:    	publishes wheel package as to ECR release artifacts"

env-file:
	# populating build/env
	@rm -f build/env
	@echo "IMAGE_NAME=${IMAGE_NAME}" >> build/env
	@echo "ECR_URL=${ECR_URL}" >> build/env
	@echo "MAJOR=${SEMVER_MAJOR}" >> build/env
	@echo "MINOR=${SEMVER_MINOR}" >> build/env
	@echo "PATCH=${SEMVER_PATCH}" >> build/env
	@echo "GIT_COMMIT=${GIT_COMMIT}" >> build/env
	cat build/env
	cp build/env build/docker/.env

clean:
	rm -rf env dist/* dbricks_utils.*

ecr-login:
	`aws ecr get-login --region ${ECR_REGION} --registry-ids ${ECR_ACCOUNT} --no-include-email`

build: env-file
	python setup.py sdist bdist_wheel

publish-dev: env-file
	$(eval export WHL_CLI_TAG := $(IMAGE_NAME)_dev_whl_cli)
	docker build --target whl-cli --tag $(WHL_CLI_TAG) --file build/docker/Dockerfile .
	mkdir -p dist/
	docker cp `docker create $(WHL_CLI_TAG)`:/opt/utils/${PACKAGE_WHL} ${PACKAGE_WHL}
	echo "Uploading ${PACKAGE_WHL} to ${PACKAGE_WHL_DEV_PATCH}"
	aws s3 cp ${PACKAGE_WHL} ${PACKAGE_WHL_DEV_PATCH}
	echo "Uploading ${PACKAGE_WHL} to ${PACKAGE_WHL_DEV_LATEST}"
	aws s3 cp ${PACKAGE_WHL} ${PACKAGE_WHL_DEV_LATEST}

publish-release: env-file
	$(eval export WHL_CLI_TAG := $(IMAGE_NAME)_dev_whl_cli)
	docker build --target whl-cli --tag $(WHL_CLI_TAG) --file build/docker/Dockerfile .
	mkdir -p dist/
	docker cp `docker create $(WHL_CLI_TAG)`:/opt/utils/${PACKAGE_WHL} ${PACKAGE_WHL}
	echo "Uploading ${PACKAGE_WHL} to ${PACKAGE_WHL_REL_PATCH}"
	aws s3 cp ${PACKAGE_WHL} ${PACKAGE_WHL_REL_PATCH}
	echo "Uploading ${PACKAGE_WHL} to ${PACKAGE_WHL_REL_LATEST}"
	aws s3 cp ${PACKAGE_WHL} ${PACKAGE_WHL_REL_LATEST}
