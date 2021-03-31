build_wheel:
	python setup.py sdist bdist_wheel

upload_wheel:
	aws s3 cp dist/ s3://intelematics-dac-build-release-artifacts/dbsutils/ --recursive
