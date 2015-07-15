# from mpl_toolkits.basemap import Basemap
from prettytable import PrettyTable
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import numpy as np
import fiona  # handling ESRI shape format
from shapely.wkt import loads as loads
from shapely.geometry import mapping, Polygon, box
import weakref
import re
from SQLOperations import *


class Region:
    """
    Define region object, instances can be passed to Query() in order to set
    boundaries
    """

    def __init__(self, name=None, boundary=None):
        self.name = name
        self.bounds = None
        self.boundary_polygon = None

        self.set_boundaries(boundary)

    def set_boundaries(self, boundary):
        """
        Set boundary bbox
        :return:
        """
        if type(boundary) == str:
            # WKT-String formatted polygon
            if boundary.startswith("POLYGON\(\("):
                self.boundary_polygon = loads(boundary)
                self.bounds = self.boundary_polygon.bounds
                self.boundary_polygon = self.boundary_polygon.wkt
            # Filepath to ESRI shape file
            elif boundary.endswith(".shp"):
                with fiona.open(boundary, 'r') as source:
                    # import as shapely polygon
                    self.boundary_polygon = Polygon(
                        next(source)['geometry']['coordinates'][0])
                self.bounds = self.boundary_polygon.bounds
                self.boundary_polygon = self.boundary_polygon.wkt
            # Link to database table containing boundary polygon
            # format: [schema].[table]
            elif re.match(r"[a-zA-Z0-9]*[.]*[a-zA-Z0-9]", boundary):
                self.bounds = boundary
                self.boundary_polygon = None
            else:
                print(
                    "Warning: Wrong or unknown input format of boundary, returning NoneValue...")
                self.bounds = None
                self.boundary_polygon = None
        # tuple containing bbox, format: (xmin, ymin, xmax, ymax)
        elif type(boundary) == tuple:
            self.bounds = boundary
            self.boundary_polygon = box(*boundary)
            self.boundary_polygon = self.boundary_polygon.wkt
        else:
            print(
                "Warning: Wrong or unknown input format of boundary, returning NoneValue...")
            self.bounds = None
            self.boundary_polygon = None


class Query:
    instances = {}

    def __init__(self, name=None, region=Region()):
        self.query_name = name
        self.__class__.instances[self.query_name] = weakref.proxy(self)
        self.results = []
        self.geom_type = None

        self.bounds = region.bounds
        self.boundary_polygon = region.boundary_polygon

    def prepare_where_query_experimental(self, features):
        """
        Create SQL query from given query features
        :rtype : string
        :param region: instance of Region() defining query boundaries
        :param features: dictionary defining basic query features
        :return: sql query
        """

        self.geom_type = features['geom_type']
        self.SRID = features['SRID']
        self.select_cols = features['select_cols']
        self.__query_features = features

        if self.bounds:
            self.__query_features['xmin'], self.__query_features['ymin'], \
            self.__query_features['xmax'], self.__query_features['ymax'] = self.bounds
        else:
            self.__query_features['xmin'] = None
            self.__query_features['ymin'] = None
            self.__query_features['xmax'] = None
            self.__query_features['ymax'] = None

        # SELECT...
        self.sql_query = "SELECT %(select_cols)s, ST_AsText(ST_Transform(%(geom_col)s,%(SRID)s))"

        # FROM/WHERE...
        # bbox of format (xmin, ymin, xmax, ymax)
        if type(self.bounds) == tuple:
            # FROM...
            self.sql_query += " FROM %(schema)s.%(relation)s"
            if features['where_cond']:
                self.sql_query += " WHERE %(where_cond)s AND"
            self.sql_query += " ST_AsText(ST_Transform(%(geom_col)s,%(SRID)s)) "
            self.sql_query += "&& ST_MakeEnvelope(%(xmin)s,(%(ymin)s,(%(xmax)s,(%(ymax)s"
        # Link to DB relation
        elif type(self.bounds) == str:
            # FROM...
            self.sql_query += " FROM %(schema)s.%(relation)s, %(clip_relation)s as clip_relation"
            if features['where_cond']:
                self.sql_query += " WHERE %(where_cond)s AND"
            self.sql_query += " ST_Contains(clip_relation.geom, ST_Transform(way,%(SRID)s))"
        # No clipping boundary
        else:
            if features['where_cond']:
                self.sql_query += " WHERE %(where_cond)s"

    def create_where_query(self, features):
        # INSECURE => SQL-INJECTIONS possible!
        """
        Create SQL query from given query features
        :rtype : string
        :param region: instance of Region() defining query boundaries
        :param features: dictionary defining basic query features
        :return: sql query
        """

        # SELECT...
        self.sql_query = "SELECT {sel_cols}, ST_AsText(ST_Transform({geom},{SRID}))".format(
            sel_cols=', '.join(features['select_cols']),
            geom=features['geom_col'],
            SRID=features['SRID'])

        # FROM/WHERE...
        # bbox of format (xmin, ymin, xmax, ymax)
        if type(self.bounds) == tuple:
            # FROM...
            self.sql_query += " FROM {schema}.{relation}".format(
                schema=features['schema'],
                relation=features['relation'])
            if features['where_cond']:
                self.sql_query += " WHERE {where_cond} AND".format(
                    where_cond=features['where_cond'])
            self.sql_query += " ST_AsText(ST_Transform({geom},{SRID})) ".format(
                geom=features['geom_col'],
                SRID=features['SRID'])
            self.sql_query += "&& ST_MakeEnvelope({},{},{},{})".format(
                *self.bounds)
        # Link to DB relation
        elif type(self.bounds) == str:
            # FROM...
            self.sql_query += " FROM {schema}.{relation}, {clip_relation} as clip_relation".format(
                schema=features['schema'],
                relation=features['relation'],
                clip_relation=self.bounds)
            if features['where_cond']:
                self.sql_query += " WHERE {where_cond} AND".format(
                    where_cond=features['where_cond'])
            self.sql_query += " ST_Contains(clip_relation.geom, ST_Transform(way,{SRID}))".format(
                SRID=features['SRID'])
        # No clipping boundary
        else:
            if features['where_cond']:
                self.sql_query += " WHERE {where_cond}".format(
                    where_cond=features['where_cond'])

        self.geom_type = features['geom_type']
        self.SRID = features['SRID']
        self.select_cols = features['select_cols']

    def clip_view2poly(self):
        """
        Clips list of n-tuples as result of SELECT-query to boundary of a given shapely polygon object
        :rtype : list
        :param view: list of n-tuples, result of SELECT-query, last element of row has to be a geometry object
        :param poly: shapely polygon object
        :return: list of n-tuples within polygon
        """

        # Todo
        # -> Type check oder duck type
        # -> Check if this is 'real' clipping... => IT IS NOT!!!
        # -> ...worse: [].intersection(...) does not work, 'Assertion failed'-error

        if self.boundary_polygon:
            coll = []
            for row in self.results:
                geom = loads(row['geom'])
                if geom.within(loads(self.boundary_polygon)):
                    coll.append(row)
            self.results = coll

    def fetch_geoms(self, source_db):
        """
        Fetches geometries and tags from PostGIS DB and clip to given boundary if such has been supplied
        :rtype : list of dictionaries
        :param datasource: Dictionary containing db specs (db, user, host, port)
        :param boundary: Clip target (bbox or shape file)
        :return: Query results
        """

        # Fetch features from PostGIS DB
        with DBOperations(**source_db) as conn:
            # view = conn.execute_query(query=self.sql_query,
            #                           params=self.__query_features)
            view = conn.execute_query(self.sql_query)

        # Transform results ('view') to list of dictionaries
        for row in view:
            dictionary = {'properties': {}}
            for i, tag in enumerate(self.select_cols):
                dictionary['properties'][tag] = row[i]
            dictionary['geom'] = row[-1]
            self.results.append(dictionary)

        # Clip features to polygon - if supplied
        if self.boundary_polygon:
            self.clip_view2poly()

        # Print number of fetched elements
        print("\n => Fetched {n} elements\n".format(n=len(self.results)))

    def print_results(self):
        """
        Print fetched results using PrettyTable
        """
        try:
            t = PrettyTable(self.results[0]['properties'].keys())
            if len(self.results) <= 1000:
                [t.add_row(row['properties'].values()) for row in self.results]
                print(t)
            else:
                [t.add_row(row['properties'].values())
                 for row in self.results[0:1000]]
                print(t)
                print("(List truncated)")
        except IndexError:
            print("No Results to display!")

    def export2shp(self, filepath):
        """
        Save results to hard disk
        :rtype : None
        :param filepath: output path
        :return:
        """

        def ESRI_schema_from_view():
            """
            :rtype: dict
            :return: Dictionary in ESRI shape format style
            """

            schema = {}
            schema['geometry'] = loads(self.results[0]['geom']).type
            schema['properties'] = {}
            # Initially set every type to NoneValue
            for key in self.results[0]['properties']:
                schema['properties'][key] = None
            # Go through result rows and if type!=None set type(value) as schema
            # type
            for r in self.results:
                for key in r['properties']:
                    if r['properties'][key]:
                        schema['properties'][key] = type(
                            r['properties'][key]).__name__
                    else:
                        schema['properties'][key] = 'str'
            return schema

        # Save result to disk using fiona-package
        if not self.results == []:
            # Create schema for ESRI-shape export
            schema = ESRI_schema_from_view()
            with fiona.collection(filepath, 'w',
                                  'ESRI Shapefile', schema) as output:
                for row in self.results:
                    output.write({
                        'properties': row['properties'],
                        'geometry': mapping(loads(row['geom']))})
            print("Saved file to {fp}".format(fp=filepath))
        else:
            print("Nothing to save - empty view!")
