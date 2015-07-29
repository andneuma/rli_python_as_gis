"""
Requirements:

* Python3
* Matplotlib with Basemap-support (http://matplotlib.org/basemap/)

Package containing various methods for conveniently receiving and visualising
geoinformation from a PostGIS/Postgresql database or web services providing
openstreetmap data

Study usage_examples.py for some working examples (apply to your local database
infrastructure!)
"""

from mpl_toolkits.basemap import Basemap
from prettytable import PrettyTable
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import numpy as np
import fiona  # handling ESRI shape format
from shapely.wkt import loads
from shapely.geometry import mapping
from simple_log import *
from SQLOperations import *
from region import *

logger = SimpleLogger(module_name="PostGISHelpers")

class Query:
    instances = {}  # Instance collector

    def __init__(self, name=None, region=Region(), debug_level='i'):

        self.query_name = name
        self.__class__.instances[self.query_name] = weakref.proxy(self)
        self.results = []
        self.region = region
        self.geom_type = None

        logger.set_debug_level(debug_level)
        
    def create_where_query(self,
                           relation,
                           schema="public",
                           select_cols="*",
                           geom_col='way',
                           where_cond=None,
                           SRID=4326):
        """
        Automatically generate and set a select/from/where SQL statement
        from given query features
        :param schema: DB schema to query
        :param relation: table name
        :param select_cols: table columns to select from
        :param where_cond: where condition for query
        :param SRID: Spatial Reference ID
        """

        # SELECT...
        self._sql_query = "SELECT {sel_cols}, ST_AsText(ST_Transform({geom},{SRID}))".format(
            sel_cols=', '.join(select_cols),
            geom=geom_col,
            SRID=SRID)

        # FROM/WHERE...
        # bbox of format (xmin, ymin, xmax, ymax)
        # if type(self.region.bounds) == tuple:
        if self.region.boundary_polygon:
            # FROM...
            self._sql_query += " FROM {schema}.{relation}".format(
                schema=schema,
                relation=relation)
            if where_cond:
                self._sql_query += " WHERE {where_cond} AND".format(
                    where_cond=where_cond)
            self._sql_query += " ST_Contains(ST_GeomFromText('{clip_pattern}',{SRID}), ST_Transform(way,{SRID}))".format(
                clip_pattern=self.region.boundary_polygon,
                SRID=SRID)
        # Link to DB relation
        elif type(self.region.bounds) == str:
            # FROM...
            self._sql_query += " FROM {schema}.{relation}, {clip_relation} as clip_relation".format(
                schema=schema,
                relation=relation,
                clip_relation=self.region.bounds)
            if where_cond:
                self._sql_query += " WHERE {where_cond} AND".format(
                    where_cond=where_cond)
            self._sql_query += " ST_Contains(ST_Transform(clip_relation.geom,{SRID}), ST_Transform(way,{SRID}))".format(
                SRID=SRID)
        # No clipping boundary
        else:
            self._sql_query += " FROM {schema}.{relation}".format(
                schema=schema,
                relation=relation)
            if where_cond:
                self._sql_query += " WHERE {where_cond}".format(
                    where_cond=where_cond)

        self.SRID = SRID
        self.select_cols = select_cols

    def insert_custom_query(self, query_text):
        """
        Set custom SQL statement
        :param query_text:
        """
        self._sql_query = query_text

    def clip_view2poly(self):
        """
        Clips list of n-tuples as result of SELECT-query to boundary of a given
        instance of Region()
        """
        # Todo
        # No real clipping -> [].intersection(...) does not work, 'Assertion failed'-error
        coll = []
        for row in self.results:
            geom = loads(row['geom'])
            if geom.within(loads(self.region.boundary_polygon)):
                coll.append(row)
        self.results = coll

    def string2psycopg_features(self, db_string):
        """
        Convert input string containing DB access information in SQLAlchemy
        style format ('user@host:port/database-name')
        :rtype : dict
        :return : dictionary of DB access information
        """
        features = re.split(r":|@|/", db_string)
        try:
            source_db = {'db': features[3],
                         'host': features[1],
                         'port': features[2],
                         'user': features[0]}
            return source_db
        except IndexError:
            logger.printmessage.error(
                "Please provide DB access information as string 'user@host:port/db'")

    def fetch_geoms(self, source_db):
        """
        Fetches items from PostGIS DB and clips results to boundary of supplied
        Region object instance
        :param source_db: String containing information on where to fetch data
        from
        :return : List of dictionary with keys 'geom' (containing WKT-formatted
        geometries) and '
        """
        logger.printmessage.info(
            "Querying DATABASE for {geoms}s...(may take some time!)".format(
                geoms=self.geom_type))
        ts = datetime.datetime.now()

        # Fetch features from PostGIS DB
        with DBOperations(**self.string2psycopg_features(source_db)) as conn:
            view = conn.execute_query(self._sql_query)

        td = datetime.datetime.now() - ts

        sec = int(td.seconds % 60)
        min = int((td.seconds - sec) / 60)

        # Print number of fetched elements
        logger.printmessage.info(
            "Fetched {n} {geoms}(s) in {td_min}m:{td_sec}s\n".format(
                n=len(view),
                geoms=self.geom_type, td_min=min,
                td_sec=sec))


        # Transform results ('view') to list of dictionaries
        for row in view:
            dictionary = {'properties': {}}
            for i, tag in enumerate(self.select_cols):
                dictionary['properties'][tag] = row[i]
            dictionary['geom'] = row[-1]
            self.results.append(dictionary)

    def print_results(self, n=1000):
        """
        Print fetched results as nicely formatted table
        :param n: Limit of result rows to display
        """
        try:
            t = PrettyTable(self.results[0]['properties'].keys())
            if len(self.results) <= n:
                [t.add_row(row['properties'].values()) for row in self.results]
                print(t)
            else:
                [t.add_row(row['properties'].values())
                 for row in self.results[0:n]]
                print(t)
                print("(List truncated to {x} elements)".format(x=n))
        except IndexError:
            logger.printmessage.warning("No Results to display!")

    def _get_vectors_from_postgis_map(self, bm, geom):
        """
        Create vector collection from given shapely geometries
        Protected method used by self.plot_view()
        :rtype : list
        :param bm: Basemap() object
        :param geom: shapely geometry object
        :return:
        """
        vectors = []
        # Try handling input as Point, Polygon, Linestring, ...
        try:
            for el in geom:
                try:
                    coords = list(el.coords)
                except NotImplementedError:
                    coords = list(list(el.exterior.coords))

                seg = []
                for coord in coords:
                    seg.append(bm(coord[0], coord[1]))
                vectors.append(np.asarray(seg))
        # If TypeError try accessing structure of Multi*-objects (MultiPolygon, ...)
        except TypeError:
            seg = []
            try:
                coords = list(geom.coords)
            except NotImplementedError:
                coords = list(geom.exterior.coords)

            for coord in coords:
                seg.append(bm(coord[0], coord[1]))
            vectors.append(np.asarray(seg))
        return vectors

    def bbox_of_view(self, results):
        """
        Calculate the bounding box of query results if no further regional
        information have been supplied (Region.bounds = None)
        :rtype : dict
        :param results: return dict of fetch_geoms(...)
        :return: bbox dict {'xmin':float, 'xmax':float, ...}
        """
        try:
            # Define initial values for view's bbox
            geom_shapely = loads(results[0]['geom'])
            geom_bounds = geom_shapely.bounds
            bbox = {'xmin': geom_bounds[0],
                    'ymin': geom_bounds[1],
                    'xmax': geom_bounds[2],
                    'ymax': geom_bounds[3]}
        except:
            logger.printmessage.warning("Error: Empty view, cannot plot any results!")
            return None

        for result in results:
            geom_shapely = loads(result['geom'])
            try:
                if min(geom_shapely.xy[0]) < bbox['xmin']:
                    bbox['xmin'] = min(geom_shapely.xy[0])
                if min(geom_shapely.xy[1]) < bbox['ymin']:
                    bbox['ymin'] = min(geom_shapely.xy[1])
                if max(geom_shapely.xy[0]) > bbox['xmax']:
                    bbox['xmax'] = max(geom_shapely.xy[0])
                if max(geom_shapely.xy[1]) > bbox['ymax']:
                    bbox['ymax'] = max(geom_shapely.xy[1])
            # Catch exception if type=Polygon
            except NotImplementedError:
                if min(geom_shapely.exterior.xy[0]) < bbox['xmin']:
                    bbox['xmin'] = min(geom_shapely.exterior.xy[0])
                if min(geom_shapely.exterior.xy[1]) < bbox['ymin']:
                    bbox['ymin'] = min(geom_shapely.exterior.xy[1])
                if max(geom_shapely.exterior.xy[0]) > bbox['xmax']:
                    bbox['xmax'] = max(geom_shapely.exterior.xy[0])
                if max(geom_shapely.exterior.xy[1]) > bbox['ymax']:
                    bbox['ymax'] = max(geom_shapely.exterior.xy[1])

        return (bbox['xmin'], bbox['ymin'], bbox['xmax'], bbox['ymax'])

    def _prepare_plot(self, resolution="i"):
        """
        Prepare basemap for plotting results of some query
        :rtype: Basemap
        :param resolution: Set basemap resolution / area threshold that shall
        still be displayed (see Basemap documentation for further details)
        :return: Basemap
        """
        # Determine bounding box if no clipping boundary was supplied
        if not self.region.bounds or type(self.region.bounds) == str:
            if isinstance(self, OSMCollection):
                if hasattr(self, 'Points'):
                    self.region.bounds = self.bbox_of_view(self.Points.results)
                elif hasattr(self, 'Lines'):
                    self.region.bounds = self.bbox_of_view(self.Lines.results)
                elif hasattr(self, 'Polygons'):
                    self.region.bounds = self.bbox_of_view(
                        self.Polygons.results)
            else:
                self.region.bounds = self.bbox_of_view(self.results)

        bbox = {'xmin': self.region.bounds[0],
                'ymin': self.region.bounds[1],
                'xmax': self.region.bounds[2],
                'ymax': self.region.bounds[3]}

        m = Basemap(resolution=resolution,
                    projection='merc',
                    llcrnrlat=bbox['ymin'] - 0.02,
                    urcrnrlat=bbox['ymax'] + 0.02,
                    llcrnrlon=bbox['xmin'] - 0.02,
                    urcrnrlon=bbox['xmax'] + 0.02,
                    lat_ts=(bbox['xmin'] +
                            bbox['xmax']) / 2)
        m.drawcoastlines()
        m.drawlsmask(land_color='white', ocean_color='aqua', lakes=True)
        m.drawcountries()

        return m

    def _collect_geoms(self, query_object, ax, m, el_limit=5000):
        """
        Create vectors for different geom and multi-geom types
        :rtype: subplot
        :param query_object:
        :param ax: Instance of plot.subplot()
        :param m: Instance of Basemap()
        :param el_limit: Maximum number of elements to display on map
        :return: subplot instance
        """
        # Collect fetched geometries
        if not len(query_object.results) > el_limit:
            try:
                for el in query_object.results:
                    vectors = query_object._get_vectors_from_postgis_map(m,
                                                                         loads(
                                                                             el[
                                                                                 'geom']))
                    lines = LineCollection(vectors, antialiaseds=(1,))
                    if not query_object.geom_type == 'LineString':
                        lines.set_facecolors('red')
                    lines.set_linewidth(0.25)
                    ax.add_collection(lines)
            # If AttributeError assume geom_type 'Point', simply collect all
            # points and perform scatterplot
            except AttributeError:
                xy = m(
                    [loads(point['geom']).x for point in query_object.results],
                    [loads(point['geom']).y for point in query_object.results])
                ax.scatter(xy[0], xy[1])

                # # Add clipping border
                # if self.region.boundary_polygon:
                #     vectors = self.__get_vectors_from_postgis_map(m, loads(
                #         self.region.boundary_polygon))
                #     border = LineCollection(vectors, antialiaseds=(1,))
                #     border.set_edgecolors('black')
                #     border.set_linewidth(1)
                #     border.set_linestyle('dashed')
                #     ax.add_collection(border)

        else:
            logger.printmessage.error("Error: >5000 elements to plot!")

        return ax

    def plot_view(self, resolution='i', el_limit=5000):
        """
        Show map of fetched geometries
        :param resolution: Set basemap resolution / area threshold that shall
        still be displayed
        :param el_limit: Maximum number of elements to display on map
        """
        ax = plt.subplot(111)
        m = self._prepare_plot(resolution=resolution)
        self._collect_geoms(self, ax, m, el_limit=el_limit)

        plt.title("{name} - total: {n} {geoms}(s)".format(
            name=self.query_name, n=len(self.results), geoms=self.geom_type))
        plt.show()

    def export2shp(self, filepath):
        """
        Save results to hard disk
        :param filepath: output path
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
            logger.printmessage.info("Saved file to {fp}".format(fp=filepath))
        else:
            logger.printmessage.warning("Nothing to save - empty view!")  ##


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

    @property
    def area_sum(self):
        """
        :rtype : float
        Calculate area of polygons in Polygons()
        :return: Sum of polygon areas
        """
        area_sum = 0
        for polygon in self.results:
            area_sum += loads(polygon['geom']).area
        return area_sum


class OSMQuery(Query):
    def __init__(self, name=None, region=Region()):
        super(OSMQuery, self).__init__(name=name, region=region)


class OSMPoints(OSMQuery, Points):
    def __init__(self, name, region=Region()):
        super(OSMPoints, self).__init__(name=name, region=region)


class OSMLines(OSMQuery, Lines):
    def __init__(self, name, region=Region()):
        super(OSMQuery, self).__init__(name=name, region=region)


class OSMPolygons(OSMQuery, Polygons):
    def __init__(self, name, region=Region()):
        super(OSMQuery, self).__init__(name=name, region=region)


class OSMCollection(OSMQuery):
    def __init__(self, name=None,
                 region=Region(),
                 points=True,
                 lines=True,
                 polygons=True):
        super(OSMQuery, self).__init__(name=name, region=region)

        if points:
            self.Points = OSMPoints(name=name, region=self.region)
        if lines:
            self.Lines = OSMLines(name=name, region=self.region)
        if polygons:
            self.Polygons = OSMPolygons(name=name, region=self.region)

    def create_collection_query(self,
                                relation_prefix,
                                schema="public",
                                select_cols="*",
                                geom_col='way',
                                where_cond=None,
                                SRID=4326):
        """
        Automatically generate and set a collective select/from/where SQL
        statement from given query features for a collection of OSMPoints and/or
        OSMLines and/or OSMPolygons
        :param schema: DB schema to query
        :param relation_prefix: OSM table name prefix (suffix is being added
        automatically)
        :param select_cols: Table columns to select from
        :param where_cond: Where condition for query
        :param geom_col: Column containing geometries
        :param SRID: Spatial Reference ID
        """
        args = locals()
        args.__delitem__('self')
        args.__delitem__('relation_prefix')

        if hasattr(self, 'Points'):
            args['relation'] = relation_prefix + "_point"
            self.Points.create_where_query(**args)

        if hasattr(self, 'Lines'):
            args['relation'] = relation_prefix + "_line"
            self.Lines.create_where_query(**args)

        if hasattr(self, 'Polygons'):
            args['relation'] = relation_prefix + "_polygon"
            self.Polygons.create_where_query(**args)

    def fetch_OSM_collection(self,
                             source_db):
        """
        Collectively fetch OSMCollection containing Points and/or Lines and/or
        Polygons
        :param source_db: DB to fetch data from
        :return:
        """
        if hasattr(self, 'Points'):
            self.Points.fetch_geoms(source_db)
        if hasattr(self, 'Lines'):
            self.Lines.fetch_geoms(source_db)
        if hasattr(self, 'Polygons'):
            self.Polygons.fetch_geoms(source_db)

    def plot_view(self, resolution='i', el_limit=5000):
        """
        METHOD OVERRIDING: Plot collected geometries of OSMCollection
        :param resolution: Set basemap resolution / area threshold that shall
        still be displayed
        :param el_limit: Maximum number of elements to display on map
        """
        ax = plt.subplot(111)
        m = self._prepare_plot(resolution=resolution)
        if hasattr(self, 'Points'):
            self._collect_geoms(self.Points, ax, m, el_limit=el_limit)
        if hasattr(self, 'Lines'):
            self._collect_geoms(self.Lines, ax, m, el_limit=el_limit)
        if hasattr(self, 'Polygons'):
            self._collect_geoms(self.Polygons, ax, m, el_limit=el_limit)

        plt.title("{name} - total: {n} {geoms}(s)".format(
            name=self.query_name, n=len(self.results), geoms=self.geom_type))
        plt.show()
