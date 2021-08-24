# Bricklayer

A Databricks utility for data engineers whose job is to farm jobs, build map layers, and other structures with data-bricks.

## Install

```
pip install https://github.com/intelematics/bricklayer/releases/download/0.0.7/bricklayer-0.0.7-py3-none-any.whl
```

## Usage

To render map display in a Databricks notebook with features:
```
%py

from bricklayer.api import DBSApi
from bricklayer.display.map import Layer, Map
import geopandas as gp

traffic_lights = gp.read_file('https://opendata.arcgis.com/datasets/1f3cb954526b471596dbffa30e56bb32_0.geojson')

Map([
  Layer(traffic_lights, color='blue', radius=5),
]).render()

```

To spawn concurrent jobs:
```
%py

from bricklayer.api import DBSApi

for x in range(3):
    job = DBSApi().create_job('./dummy_job')

```

## Contributing

PRs accepted.

## License

MIT ©
