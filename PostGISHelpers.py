# from mpl_toolkits.basemap import Basemap
from prettytable import PrettyTable
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import numpy as np
import fiona  # handling ESRI shape format
from shapely.wkt import loads as loads
from shapely.geometry import mapping, Polygon, box

from SQLOperations import *
from region import *


class Query:
    instances = {}

    def __init__(self, name=None, region=Region()):
        self.query_name = name
        self.__class__.instances[self.query_name] = weakref.proxy(self)
        self.results = []
        self.geom_type = None
        self.sql_query = None

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
            self.__query_features['xmax'], self.__query_features[
                'ymax'] = self.bounds
        else:
            self.__query_features['xmin'] = None
            self.__query_features['ymin'] = None
            self.__query_features['xmax'] = None
            self.__query_features['ymax'] = None

        # SELECT...
        self._sql_query = "SELECT %(select_cols)s, ST_AsText(ST_Transform(%(geom_col)s,%(SRID)s))"

        # FROM/WHERE...
        # bbox of format (xmin, ymin, xmax, ymax)
        if type(self.bounds) == tuple:
            # FROM...
            self._sql_query += " FROM %(schema)s.%(relation)s"
            if features['where_cond']:
                self._sql_query += " WHERE %(where_cond)s AND"
            self._sql_query += " ST_AsText(ST_Transform(%(geom_col)s,%(SRID)s)) "
            self._sql_query += "&& ST_MakeEnvelope(%(xmin)s,(%(ymin)s,(%(xmax)s,(%(ymax)s"
        # Link to DB relation
        elif type(self.bounds) == str:
            # FROM...
            self._sql_query += " FROM %(schema)s.%(relation)s, %(clip_relation)s as clip_relation"
            if features['where_cond']:
                self._sql_query += " WHERE %(where_cond)s AND"
            self._sql_query += " ST_Contains(clip_relation.geom, ST_Transform(way,%(SRID)s))"
        # No clipping boundary
        else:
            if features['where_cond']:
                self._sql_query += " WHERE %(where_cond)s"

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
        self._sql_query = "SELECT {sel_cols}, ST_AsText(ST_Transform({geom},{SRID}))".format(
            sel_cols=', '.join(features['select_cols']),
            geom=features['geom_col'],
            SRID=features['SRID'])

        # FROM/WHERE...
        # bbox of format (xmin, ymin, xmax, ymax)
        if type(self.bounds) == tuple:
            # FROM...
            self._sql_query += " FROM {schema}.{relation}".format(
                schema=features['schema'],
                relation=features['relation'])
            if features['where_cond']:
                self._sql_query += " WHERE {where_cond} AND".format(
                    where_cond=features['where_cond'])
            self._sql_query += " ST_AsText(ST_Transform({geom},{SRID})) ".format(
                geom=features['geom_col'],
                SRID=features['SRID'])
            self._sql_query += "&& ST_MakeEnvelope({},{},{},{})".format(
                *self.bounds)
        # Link to DB relation
        elif type(self.bounds) == str:
            # FROM...
            self._sql_query += " FROM {schema}.{relation}, {clip_relation} as clip_relation".format(
                schema=features['schema'],
                relation=features['relation'],
                clip_relation=self.bounds)
            if features['where_cond']:
                self._sql_query += " WHERE {where_cond} AND".format(
                    where_cond=features['where_cond'])
            self._sql_query += " ST_Contains(clip_relation.geom, ST_Transform(way,{SRID}))".format(
                SRID=features['SRID'])
        # No clipping boundary
        else:
            if features['where_cond']:
                self._sql_query += " WHERE {where_cond}".format(
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
        # No real clipping -> [].intersection(...) does not work, 'Assertion failed'-error
        if self.boundary_polygon:
            coll = []
            for row in self.results:
                geom = loads(row['geom'])
                if geom.within(loads(self.boundary_polygon)):
                    coll.append(row)
            self.results = coll

    def fetch_geoms(self, source_db):
        def string2psycopg_features(source_db):
            """
            :rtype : dict
            :return :
            """
            features = re.split(r":|@|/", source_db)
            try:
                source_db = {'db': features[3],
                             'host': features[1],
                             'port': features[2],
                             'user': features[0]}
                return source_db
            except IndexError:
                print(
                    "Please provide DB access information as string 'user@host:port/db'")

        """
        Fetches geometries and tags from PostGIS DB and clip to given boundary if such has been supplied
        :rtype : list of dictionaries
        :param datasource: Dictionary containing db specs (db, user, host, port)
        :param boundary: Clip target (bbox or shape file)
        :return: Query results
        """

        # Fetch features from PostGIS DB
        with DBOperations(**string2psycopg_features(source_db)) as conn:
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
            print("Nothing to save - empty view!")  ##


class Points(Query):
    def __init__(self, name, region=Region()):
        super(Points, self).__init__(name=name, region=region)
        self.geom_type = 'Point'


class Lines(Query):
    def __init__(self, name, region=Region()):
        super(Lines, self).__init__(name=name, region=region)
        self.geom_type = 'LineString'


class Polygons(Query):
    def __init__(self, name, region=Region()):
        super(Polygons, self).__init__(name=name, region=region)
        self.geom_type = 'Polygon'


class OSMQuery(Query):
    def __init__(self, name=None, region=Region()):
        super(OSMQuery, self).__init__(name=name, region=region)

    # Override 'create_where_query'-method of Query superclass in order to
    # take care of OSM specifics within SQL-statement
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
        self._sql_query = "SELECT {sel_cols}, ST_AsText(ST_Transform(way,{SRID}))".format(
            sel_cols=', '.join(features['select_cols']),
            SRID=features['SRID'])

        # FROM/WHERE...
        # bbox of format (xmin, ymin, xmax, ymax)
        if type(self.bounds) == tuple:
            # Clip outside PostGIS-DB using external clip relation
            self._sql_query += " FROM public.{relation}".format(
                relation=features['relation'])
            if features['where_cond']:
                self._sql_query += " WHERE {where_cond} AND".format(
                    where_cond=features['where_cond'])
            self._sql_query += " ST_AsText(ST_Transform(way,{SRID})) ".format(
                SRID=features['SRID'])
            self._sql_query += "&& ST_MakeEnvelope({},{},{},{})".format(
                *self.bounds)
        # Link to DB relation
        elif type(self.bounds) == str:
            # Clip within PostGIS-DB using internal clip relation
            self._sql_query += " FROM public.{relation}, {clip_relation} as clip_relation".format(
                relation=features['relation'],
                clip_relation=self.bounds)
            if features['where_cond']:
                self._sql_query += " WHERE {where_cond} AND".format(
                    where_cond=features['where_cond'])
            self._sql_query += " ST_Contains(clip_relation.geom, ST_Transform(way,{SRID}))".format(
                SRID=features['SRID'])
        # No clipping boundary
        else:
            self._sql_query += " FROM public.{relation}".format(
                relation=features['relation'])
            if features['where_cond']:
                self._sql_query += " WHERE {where_cond}".format(
                    where_cond=features['where_cond'])

        self.SRID = features['SRID']
        self.select_cols = features['select_cols']


class OSMPoints(OSMQuery, Points):
    def __init__(self, name, region=Region()):
        super(OSMPoints, self).__init__(name=name, region=region)


class OSMLines(OSMQuery, Lines):
    def __init__(self, name, region=Region()):
        super(OSMQuery, self).__init__(name=name, region=region)


class OSMPolygons(OSMQuery, Polygons):
    def __init__(self, name, region=Region()):
        super(OSMQuery, self).__init__(name=name, region=region)
