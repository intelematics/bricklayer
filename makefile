build_wheel:
	python setup.py sdist bdist_wheel

upload_wheel:
	# aws --profile=default s3 cp dist/ s3://intelematics-dac-build-release-artifacts/dbricks_utils/ --recursive
	aws --profile=dev s3 cp dist/ s3://intelematics-dac-tf-dev-ci-external/tmp/dbricks_utils/ --recursive
