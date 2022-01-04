# Bricklayer Developers

Some brief guidance for developers on this repository

## Development & Release Process
- Create a `feature/xxxxxx` branch
- Do development
- Update `CHANGELOG.md`
- Update `bricklayer/__version__.py`
- Update package dependencies in `setup.py` (if required)
- Update documentation e.g. examples of new features in `examples/README.md`
- Test wheel build
- Pull request, obtain approval & merge to master
  - GitHub will run an action which builds the wheel, generates a release and publishes it

## Wheel testing
A test wheel can be built (appearing in the `dist` folder) with
 - `make clean`
 - `make build_wheel`

To install for testing on databricks:
 - The bricklayer library must be copied to an S3 location that can be accessed in a databricks "dbfs mount" (e.g. `example-s3-bucket-dbfs-scratch/bricklayer/`)
 - Copy the `.whl` file in the `dist` folder to this location (e.g. `aws s3 cp "bricklayer-0.0.12-py3-none-any.whl" "s3://example-s3-bucket-dbfs-scratch/bricklayer/"`
 - Make sure you can see the `.whl` file from databricks (e.g. `%sh ls /dbfs/mnt/dbfs_scratch/bricklayer`)
 - Install the `.whl` file on the databricks cluster from your notebook (e.g. `%pip install --force-reinstall /dbfs/mnt/dbfs_scratch/bricklayer/bricklayer-0.0.12-py3-none-any.whl`)
