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
  - GitHub will run an action which builds the wheel and generates a draft release
- Publish release (in github repo "releases" section)

## Wheel testing
A test wheel can be built (appearing in the `dist` folder) with
 - `make clean`
 - `make build_wheel`

It can be installed with (e.g. on databricks):

```
%pip install --force-reinstall <path_to_wheel>
```
