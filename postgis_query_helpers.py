## Functions to
from prettytable import PrettyTable
import psycopg2
import fiona
from shapely.wkt import loads
from shapely.geometry import mapping, Polygon
import re

## --- CLASS DEFINITIONS

class DBOperations():
    """
    Class used to cleanly handle operations on PostGIS DB
    """
    def __enter__(self):
        self.connection = psycopg2.connect(
            database=self.db_setup['db'],
            user=self.db_setup['user'],
            host=self.db_setup['host'],
            port=self.db_setup['port'])
        self.cur = self.connection.cursor()
        return self

    def __init__(self, db, host, user, password=None, port=5432):
        self.db_setup = {
            'db': db,
            'host': host,
            'port': port,
            'password': password,
            'user': user}

    def execute_query(self, query):
        try:
            self.cur.execute(query)
            results = self.cur.fetchall()
            return results
        except psycopg2.Error as e:
            print "ERROR during DB query: {e}".format(e=e.message)
            print "lalala"
            self.connection.rollback()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.connection.close()


## --- FUNCTIONS DEFINITIONS

def create_sql_query(main_dt, bbox=None):
    """
    Create SQL query from given query features
    :rtype : string
    :param main_dt: "map" file
    :param bbox: boundary box
    :return: sql query
    """
    sql_query = "SELECT {sel_cols}, ST_AsText(ST_Transform({geom},{SRID}))".format(
        sel_cols=', '.join(main_dt['query_features']['select_cols']),
        geom=main_dt['query_features']['geom_col'],
        SRID=main_dt['query_features']['SRID'])

    sql_query += " FROM {schema}.{table} ".format(
        schema=main_dt['query_features']['schema'],
        table=main_dt['query_features']['table'])

    if main_dt['query_features']['where_cond']:
        sql_query += " WHERE {where_cond}".format(
            where_cond=main_dt['query_features']['where_cond'])
        if bbox:
            sql_query += "AND ST_AsText(ST_Transform({geom},{SRID})) ".format(
                geom=main_dt['query_features']['geom_col'],
                SRID=main_dt['query_features']['SRID'])
            sql_query += "&& ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax})".format(
                **bbox)
    else:
        if bbox:
            sql_query += "WHERE ST_AsText(ST_Transform({geom},{SRID})) ".format(
                geom=main_dt['query_features']['geom_col'],
                SRID=main_dt['query_features']['SRID'])
            sql_query += "&& ST_MakeEnvelope({xmin},{ymin},{xmax},{ymax})".format(
                **bbox)

    return sql_query


def clip2poly(view, poly):
    """
    Clips list of n-tuples as result of SELECT-query to boundary of a given shapely polygon object
    :rtype : list
    :param view: list of n-tuples, result of SELECT-query, last element of row has to be a geometry object
    :param poly: shapely polygon object
    :return: list of n-tuples within polygon
    """
    coll = []
    for row in view:
        geom = loads(row[-1])
        if geom.within(poly):
            coll.append(row)
    return coll


def fetch_geoms(main_dt,
                boundary=None,
                summary_table=True):
    """
    Fetches geometries and tags from PostGIS DB and clip to given boundary if such has been supplied
    :rtype : dictionary
    :param main_dt: "map" file
    :param boundary: Clip target (bbox or shape file)
    :param summary_table: True/False, puts out the a table of view results
    :return: Query results
    """

    def boundary_type(b):
        if type(b) == str:
            if re.match("POLYGON\(\(", b):
                return 'wkt_poly'
            elif re.search("\.shp", b):
                return 'shapefile'
        elif type(b) == list and len(b) == 4:
            # Todo
            # * Maybe implement more precise format checking
            return 'bbox'
        else:
            return None

    ## Define query boundary depending on input type (None, bbox, WKT-defined Polygon or shapefile)
    if boundary_type(boundary) == None:
        # Set bbox to None if none supplied
        bbox = None
        polygon = None
        sql_query = create_sql_query(main_dt, bbox=None)
    elif boundary_type(boundary) == 'bbox':
        # Set bbox according to self-defined values
        bbox = {'xmin': boundary[0], 'ymin': boundary[1],
                'xmax': boundary[2], 'ymax': boundary[3],
                'SRID': main_dt['query_features']['SRID']}
        polygon = None
        sql_query = create_sql_query(main_dt, bbox)
    elif boundary_type(boundary) == 'wkt':
        # import as shapely polygon
        polygon = loads(boundary)
        bbox = {'xmin': polygon.bounds[0], 'ymin': polygon.bounds[1],
                'xmax': polygon.bounds[2], 'ymax': polygon.bounds[3],
                'SRID': main_dt['query_features']['SRID']}
    elif boundary_type(boundary) == 'shapefile':
        # Set bbox as canvas of supplied shapefile
        with fiona.open(boundary, 'r') as source:
            # import as shapely polygon
            polygon = Polygon(next(source)['geometry']['coordinates'][0])
            bbox = {'xmin': source.bounds[0], 'ymin': source.bounds[1],
                    'xmax': source.bounds[2], 'ymax': source.bounds[3],
                    'SRID': main_dt['query_features']['SRID']}
            sql_query = create_sql_query(main_dt, bbox)
    else:
        print "Wrong input for clipping boundary. " \
              "Supply valid boundary style (shapefile, bbox or wkt-polygon) " \
              "or set to None"
        return None


    ## Fetch features from PostGIS DB as specified in main_dt
    with DBOperations(**main_dt['db_setup']) as conn:
        view = conn.execute_query(sql_query)

    ## Clip features to polygon - if supplied
    if polygon:
        view = clip2poly(view, polygon)

    ## Print fetched results using PrettyTable(optional)
    if summary_table:
        t = PrettyTable(main_dt['query_features']['select_cols'])
        if len(view) <= 1000:
            [t.add_row(row[0:-1]) for row in view]
            print t
        else:
            [t.add_row(row[0:-1]) for row in view[1:1000]]
            print t
            print "(List truncated)"
    print "Fetched {n} elements\n".format(n=len(view))

    ## Transform results ('view') to list of dictionaries
    view_as_dict = []
    for row in view:
        dict = {}
        dict['properties'] = {}
        for i, tag in enumerate(main_dt['query_features']['select_cols']):
            dict['properties'][tag] = row[i]
        dict['geom'] = row[-1]

        view_as_dict.append(dict)

    return view_as_dict


def view2shp(view, filepath):
    """
    Save result from fetch_geoms(...) to hard disk
    :rtype : None
    :param view: resulting view from fetch_geoms(...)
    :param filepath: output path
    :return:
    """

    def ESRI_schema_from_view():
        """
        :rtype: dict
        :return: Dictionary in ESRI shape format style
        """
        schema = {}
        schema['geometry'] = loads(view[0]['geom']).type
        schema['properties'] = {}
        # Initially set every type to NoneValue
        for key in view[0]['properties'].keys():
            schema['properties'][key] = None
        # Go through result rows and if type!=None set type(value) as schema type
        for r in view:
            for key in r['properties'].keys():
                if r['properties'][key]:
                    t = type(r['properties'][key]).__name__
                    schema['properties'][key] = t
                else:
                    schema['properties'][key] = 'str'
        return schema

    # Save result to disk using fiona-package
    if not view == []:
        # Create schema for ESRI-shape export
        schema = ESRI_schema_from_view()
        with fiona.collection(filepath, 'w',
                              'ESRI Shapefile', schema) as output:
            for row in view:
                output.write({
                    'properties': row['properties'],
                    'geometry': mapping(loads(row['geom']))
                })
        print "Saved file to {fp}".format(fp=filepath)
    else:
        print "Nothing to save - empty view!"