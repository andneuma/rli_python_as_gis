# from mpl_toolkits.basemap import Basemap
from prettytable import PrettyTable
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
import numpy as np
import fiona  # handling ESRI shape format
import urllib  # Python 3 issues?
import xml.etree.ElementTree as ET  # XML-parsing
from shapely.wkt import loads as loads
from shapely.geometry import mapping, Polygon, shape

from SQLOperations import *

## --- FUNCTION DEFINITIONS

def create_where_query(main_dt, bbox=None):
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


def clip_view2poly(view, poly):
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
    coll = []
    for row in view:
        geom = loads(row[-1])
        if geom.within(poly):
            coll.append(row)
        # row[-1] = poly.intersection(geom)
    return view


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
        """
        :rtype : str
        param b : boundary
        """
        if type(b) == str:
            # if re.match("POLYGON\(\(", b):
            if b.startswith("POLYGON\(\("):
                ret_type = 'wkt_poly'
            elif b.endswith(".shp"):
                ret_type = 'shapefile'
        elif type(b) == dict:
            if set(b) == set(['xmin', 'ymin', 'xmax', 'ymax']):
                ret_type = 'bbox'
            else:
                print("Wrong input format of bbox, returning NoneValue...")
                ret_type = ''
        else:
            ret_type = ''
        return ret_type

    ## Define query boundary depending on input type (None, bbox, WKT-defined Polygon or shapefile)
    bbox = None
    polygon = None

    if boundary_type(boundary) == 'bbox':
        # Set bbox according to self-defined values
        bbox = {'xmin': boundary[0], 'ymin': boundary[1],
                'xmax': boundary[2], 'ymax': boundary[3],
                'SRID': main_dt['query_features']['SRID']}
        polygon = None
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
    else:
        print("No input for clipping boundary. Setting to None... ")
        bbox = None

    # Create SQL query
    sql_query = create_where_query(main_dt, bbox)

    ## Fetch features from PostGIS DB as specified in main_dt
    with DBOperations(**main_dt['db_setup']) as conn:
        view = conn.execute_query(sql_query)

        ## Clip features to polygon - if supplied
    if polygon:
        view = clip_view2poly(view, polygon)

    ## Print fetched results using PrettyTable(optional)
    if summary_table:
        t = PrettyTable(main_dt['query_features']['select_cols'])
        if len(view) <= 1000:
            [t.add_row(row[0:-1]) for row in view]
            print(t)
        else:
            [t.add_row(row[0:-1]) for row in view[1:1000]]
            print(t)
            print("(List truncated)")

    # Print number of fetched elements
    print("\n => Fetched {n} elements\n".format(n=len(view)))

    ## Transform results ('view') to list of dictionaries
    view_as_dict = {'bbox': bbox,
                    'geom_type': main_dt['query_features']['geom_type'],
                    'results': []}
    for row in view:
        dictionary = {'properties': {}}
        for i, tag in enumerate(main_dt['query_features']['select_cols']):
            dictionary['properties'][tag] = row[i]
        dictionary['geom'] = row[-1]
        view_as_dict['results'].append(dictionary)

    return view_as_dict

def shp2shapely(filepath):
    """
    Read local shapefile and convert to shapely object for easy processing
    :param filepath: Path to Shapefile
    :return: Shapely object
    """
    # Todo
    # -> Should have same structure as view...?
    with fiona.open(filepath) as shp:
        shape(shp[0]['geometry'])



## Helper function for plotting the results
def get_vectors_from_postgis_map(bm, geom):
    """
    Create vector collection from given shapely geometries
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


def bbox_of_view(view):
    """
    :rtype : dict
    :param view: return dict of fetch_geoms(...)
    :return: bbox dict {'xmin':float, 'xmax':float, ...}
    """
    try:
        # Define initial values for view's bbox
        geom_shapely = loads(view['results'][0]['geom'])
        geom_bounds = geom_shapely.bounds
        bbox = {'xmin': geom_bounds[0],
                'ymin': geom_bounds[1],
                'xmax': geom_bounds[2],
                'ymax': geom_bounds[3]}
    except:
        print("Error: Empty view!")
        return None

    # Todo
    # -> This part of code definately needs improvement...
    for result in view['results']:
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
    return bbox


def plot_view(result):
    # Determine bounding box if no clipping boundary was supplied
    if not result['bbox']:
        result['bbox'] = bbox_of_view(result)

    ax = plt.subplot(111)
    # plt.box(on=None)
    m = Basemap(resolution='i',
                projection='merc',
                llcrnrlat=result['bbox']['ymin'],
                urcrnrlat=result['bbox']['ymax'],
                llcrnrlon=result['bbox']['xmin'],
                urcrnrlon=result['bbox']['xmax'],
                lat_ts=(result['bbox']['xmin'] +
                        result['bbox']['xmax']) / 2)
    m.drawcoastlines()

    try:
        for el in result['results']:
            vectors = get_vectors_from_postgis_map(m, loads(el['geom']))
            lines = LineCollection(vectors, antialiaseds=(1, ))
            lines.set_facecolors('black')
            lines.set_edgecolors('white')
            lines.set_linewidth(1)
            ax.add_collection(lines)
        m.fillcontinents(color='coral', lake_color='aqua')
    # If AttributeError assume geom_type 'Point', simply collect all
    # points and perform scatterplot
    except AttributeError:
        xy = m([loads(point['geom']).x for point in result['results']],
               [loads(point['geom']).y for point in result['results']])
        plt.scatter(xy[0], xy[1])

    plt.show()


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
        schema['geometry'] = loads(view['results'][0]['geom']).type
        schema['properties'] = {}
        # Initially set every type to NoneValue
        for key in view['results'][0]['properties']:
            schema['properties'][key] = None
        # Go through result rows and if type!=None set type(value) as schema type
        for r in view['results']:
            for key in r['properties']:
                if r['properties'][key]:
                    schema['properties'][key] = type(
                        r['properties'][key]).__name__
                else:
                    schema['properties'][key] = 'str'

        return schema

    # Save result to disk using fiona-package
    if not view == []:
        # Create schema for ESRI-shape export
        schema = ESRI_schema_from_view()
        with fiona.collection(filepath, 'w',
                              'ESRI Shapefile', schema) as output:
            for row in view['results']:
                output.write({
                    'properties': row['properties'],
                    'geometry': mapping(loads(row['geom']))
                })
        print("Saved file to {fp}".format(fp=filepath))
    else:
        print("Nothing to save - empty view!")


def fetch_admin_from_latlon(lat, lon):
    """
    Receive reverse geocoded information from lat/lon point
    :rtype : dict
    :param lat: latitude
    :param lon: longitude
    :param zoom: detail level of information
    :return: dictionary
    """

    def parse_result(res):
        root = ET.fromstring(res)
        address_parts = {}

        for a in root[1]:
            address_parts[a.tag] = a.text

        return address_parts

    query = "http://nominatim.openstreetmap.org/reverse?"
    query += "format=xml"
    query += "&lat={lat}".format(lat=lat)
    query += "&lon={lon}".format(lon=lon)
    query += "&zoom=18"
    query += "&addressdetails=1"

    conn = urllib.request.urlopen(query)
    rev_geocode = conn.read()
    address_parts = parse_result(rev_geocode)

    return address_parts