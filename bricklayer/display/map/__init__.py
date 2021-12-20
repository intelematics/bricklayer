''' Module to display a folium map in databricks notebooks'''
import math

import pyspark
from pyspark.sql import SparkSession

import pandas as pd
import folium
from folium.plugins import HeatMap


import shapely.wkt as wkt
import shapely.geometry
import shapely.geometry.base


class Layer():
    ''' Layer to be rendered in the map '''

    def __init__(self, data, geometry_col=None, popup_attrs=False, color='red',
                    weight=None, radius=1):
        """
            Args:
                data (*): pandas dataframe, or a geodataframe or a spark dataframe or a databricks SQL query.
                popup_attrs (list): the attributes used to populate a pop up, if False there will be no popup. If True it will put all the attrs.
                color (str): Color to render the layer. Color name or RGB. (i.e. '#3388ff')
                weight (int): Width of the stroke when rendering lines or points. By default is 1.
                radius (int): Radius of the circles used for points default is 1.

            Returns:
                folium.Map: Folium map to be rendered.
        """
        dataframe = self.get_dataframe(data)
        if dataframe.empty:
            raise ValueError('No data to display')
        self.geometry_col = self.get_geometry_col(geometry_col, dataframe)
        self.dataframe = self.get_dataframe_with_geom(dataframe, self.geometry_col)
        self.centroid = self.get_centroid(self.dataframe, self.geometry_col)
        self.popup_attrs = popup_attrs
        self.color = color
        self.weight = weight
        self.radius = radius

    def get_geometry_col(self, geometry_col: str, dataframe: pd.DataFrame):
        '''Return the name of the geometry column'''
        if geometry_col is not None:
            if geometry_col not in dataframe.columns:
                raise ValueError(f"Column {geometry_col} not found in data columns")
            return geometry_col
        else:
            candidates = []
            for column in dataframe.columns:
                if 'geom' in column:
                    candidates.append(column)
                elif 'geography' in column:
                    candidates.append(column)
                elif 'wkt' in column:
                    candidates.append(column)
            if len(candidates) > 1:
                raise ValueError("Specify the geometry_col argument for the data")
            return candidates[0]

    def get_dataframe(self, data)->pd.DataFrame:
        '''Get the data in a pandas DataFrame'''
        if isinstance(data, pd.DataFrame):
            return data.copy()
        if isinstance(data, pyspark.sql.dataframe.DataFrame):
            return data.toPandas()
        if isinstance(data, str):
            spark = SparkSession.builder.getOrCreate()
            return spark.sql(data).toPandas()
        raise NotImplementedError(f"Can't interpret data with type {type(data)}")

    def get_dataframe_with_geom(self, dataframe: pd.DataFrame, geometry_col: str):
        '''Convert the geometry column to a shapely geometry'''
        geom = dataframe.iloc[0][geometry_col]
        if isinstance(geom, str):
            dataframe[geometry_col] = dataframe[geometry_col].apply(wkt.loads)
            return dataframe
        if isinstance(geom, shapely.geometry.base.BaseGeometry):
            return dataframe
        raise ValueError(f"Invalid type for geometry_colum in the data ({type(geom)})")

    def get_centroid(self, dataframe: pd.DataFrame, geometry_col: str):
        '''Get the centroid of all the geometries in the layer'''
        centroids = [r.centroid for _, r in dataframe[geometry_col].items()]
        multipoint = shapely.geometry.MultiPoint(centroids)
        return multipoint.centroid

    def get_popup(self, row: pd.Series):
        '''Get a folium pop-up with the requested attributes'''
        if isinstance(self.popup_attrs, list):
            non_geom_cols = self.popup_attrs
        else:
            non_geom_cols = list(self.dataframe.columns)
            non_geom_cols.remove(self.geometry_col)

        return folium.Popup((
                row
                [non_geom_cols]
                .to_frame()
                .to_html()
        ))

    def get_map_geom(self, row: pd.Series):
        '''Get folium geometry from the shapely geom'''
        sgeom = row[self.geometry_col]
        kwargs = {'color': self.color}
        if self.popup_attrs:
            html_popup = self.get_popup(row)
        else:
            html_popup = None
        if self.weight is not None:
            kwargs['weight'] = self.weight
        if isinstance(sgeom, shapely.geometry.LineString):
            coords = [(y, x) for x,y in sgeom.coords]
            fgeom = folium.PolyLine(
                coords,
                **kwargs
            )
        elif isinstance(sgeom, shapely.geometry.Point):
            kwargs['radius'] = self.radius
            coords = [(y, x) for x,y in sgeom.coords]
            fgeom = folium.CircleMarker(
                coords[0],
                **kwargs
            )
        else:
            raise NotImplementedError(f'Geometry Type not Supported {type(sgeom)}')
        if html_popup:
            fgeom.add_child(html_popup)
        return fgeom

    def get_bounds(self):
        '''Get the bounds for all the geometries'''
        minx, miny, maxx, maxy = None, None, None, None
        geoms_bounds = self.dataframe[self.geometry_col].apply(lambda g:g.bounds)
        for _minx, _miny, _maxx, _maxy in geoms_bounds:
            if minx is None:
                minx, miny, maxx, maxy = _minx, _miny, _maxx, _maxy
            else:
                minx = min(minx, _minx)
                miny = min(miny, _miny)
                maxx = max(maxx, _maxx)
                maxy = max(maxy, _maxy)
        return minx, miny, maxx, maxy

    def render_to_map(self, folium_map):
        '''Render the layer into the map'''
        for _, row in self.dataframe.iterrows():
            map_geom = self.get_map_geom(row)
            map_geom.add_to(folium_map)

class HeatMapLayer(Layer):
    '''Add a heat map layer to be rendered to the map'''

    def __init__(self, data, name=None, geometry_col=None, radius=10, blur=10, min_opacity=1, gradient={0.4: 'blue', 0.65: 'lime', 1: 'red'}):
        """
            Args:
                data (*): pandas dataframe, or a geodataframe or a spark dataframe or a databricks SQL query.
                geometry_col (str): The name of the column where the lat-long or linestring geometry is contained
                radius (int): The radius of each "point" of the heatmap
                blur (int): Amount of blur in each point
                min_opacity (int): The minimum opacity the heat will start at
                gradient (dict): Color gradient config

            Returns:
                folium.Map: Folium map to be rendered.
        """
        dataframe = Layer.get_dataframe(self, data)
        if dataframe.empty:
            raise ValueError('No data to display')
        self.geometry_col = Layer.get_geometry_col(self, geometry_col, dataframe)
        self.dataframe = Layer.get_dataframe_with_geom(self, dataframe, self.geometry_col)
        self.centroid = Layer.get_centroid(self, self.dataframe, self.geometry_col)
        self.name = name
        self.blur = blur
        self.min_opacity = min_opacity
        self.gradient = gradient
        self.radius = radius

    def get_coord_list(self):
        ''' Converts the geometry column shapely objects into a list of lat-long 2-tuples '''
        coord_list = []
        for _, row in self.dataframe.iterrows():
            geometry = row[self.geometry_col].coords
            coords = [(y, x) for x, y in geometry]
            coord_list = coord_list + coords
        return coord_list

    def render_to_map(self, folium_map):
        ''' Render the layer into the map '''
        coord_list = self.get_coord_list()
        heat_map_layer = HeatMap(
            coord_list, 
            name=self.name,
            blur=self.blur,
            min_opacity=self.min_opacity,
            gradient=self.gradient,
            radius=self.radius
        )
        heat_map_layer.add_to(folium_map)
        
class Map():
    '''Map that can render layers'''

    def __init__(self, layers: list, **map_args):
        self.layers = layers
        self.map_args = map_args.copy()
        self.map_args['zoom_start'] = self.map_args.get('zoom_start', 13)

    def get_centroid(self):
        '''Get the centroid of all the layers'''
        centroids = [layer.centroid for layer in self.layers]
        multipoint = shapely.geometry.MultiPoint(centroids)
        return multipoint.centroid

    def get_bounds(self):
        '''Get the bounds of all the layers'''
        minx, miny, maxx, maxy = None, None, None, None
        for layer in self.layers:
            _minx, _miny, _maxx, _maxy = layer.get_bounds()
            if minx is None:
                minx, miny, maxx, maxy = _minx, _miny, _maxx,_maxy
            else:
                minx = min(minx, _minx)
                miny = min(miny, _miny)
                maxx = max(maxx, _maxx)
                maxy = max(maxy, _maxy)
        return minx, miny, maxx, maxy

    def render(self):
        '''Render the map'''
        map_centroid  = self.get_centroid()
        folium_map = folium.Map(
            [map_centroid.y,map_centroid.x],
            **self.map_args
        )
        for layer in self.layers:
            layer.render_to_map(folium_map)
        return folium_map
