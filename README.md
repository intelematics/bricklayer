# Bricklayer

A Databricks utility for data engineers whose job is to farm jobs, build map layers, and other structures with data-bricks.

## Install

```

pip install https://github.com/intelematics/bricklayer/releases/download/0.0.7/bricklayer-0.0.7-py3-none-any.whl
```

## Usage

To render map display in a Databricks notebook with features:
```python
%py

from bricklayer.api import DBSApi
from bricklayer.display.map import Layer, Map
import geopandas as gp

traffic_lights = gp.read_file('https://opendata.arcgis.com/datasets/1f3cb954526b471596dbffa30e56bb32_0.geojson')

Map([
  Layer(traffic_lights, color='blue', radius=5),
]).render()

```

To spawn concurrent jobs in a Databricks notebook:
```python
%py

from bricklayer.api import DBSApi

for x in range(3):
    job = DBSApi().create_job('./dummy_job')

```
More examples can be found under [examples](examples).

## Roadmap

See [the roadmap](ROADMAP.md) if you are interested in plans for the
future.

## Changelog

Follow bricklayer's updates on [the changelog](CHANGELOG.md).

## Reporting Issues

Please report any bugs and enhancement ideas using the [bricklayer issue
tracker](https://github.com/intelematics/bricklayer/issues).

## Contributing

PRs accepted.

## License

Bricklayer is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE) for the full license text.
