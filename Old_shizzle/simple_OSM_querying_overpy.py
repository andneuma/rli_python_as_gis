##########################################
#
# Created by A. Neumann using python wrapper for Overpass API (overpy by PhiBo/Dinotools)
#
# TODO:
# * Understand Overpass QL
# * progressbar for query calls?
# => im Prinzip easy, müsste man overpy.query erweitern um progress-bar funktionalität
#
##########################################

### Import Libraries
from numpy.core.tests.test_getlimits import test_instances
from overpy_helpers import *
import codecs
from osm_helpers import *

### Define storage architecture
# Store results as object shapely object instances (points, lines, polygons,...)
class Node(object):
    def __init__(self, id, lonlat, tags):
        self.id = id
        # self.geometry = Point(projection(*lonlat))
        self.geometry = Point(*lonlat)
        self.tags = tags
        # self.storage("asd")


class Way(object):
    def __init__(self, id, waypoints, tags):
        """
        :param id: Supply some id
        :param waypoints: list of {Lon,Lat}-waypoints
        :param tags: way/line tag
        :return:
        """
        self.id = id
        self.geometry = LineString(
            [(waypoint['Lon'], waypoint['Lat'])
             for waypoint in waypoints])
        self.tags = tags

    @classmethod
    def from_overpy_obj(self, overpy_way_obj):
        """
        :param overpy_way_obj: Overpy way object
        :return: list of georeferenced nodes linked to a way of way_id
        """

        # Fetch missing nodes (Lon, Lat)
        way_nodes = overpy_way_obj.get_nodes(resolve_missing=True)

        way_nodes_conv = []
        for way_node in way_nodes:
            way_nodes_conv.append(
                {'Lon': float(way_node.lon), 'Lat': float(way_node.lat)})

        return Way(id=overpy_way_obj.id,
                   waypoints=way_nodes_conv,
                   tags=overpy_way_obj.tags)


### WORKING SCRIPT

# Overpass API: Bounding box clauses always start with the lower latitude
# followed by lower longitude, then upper latitude then upper longitude. Note
# that this is different from the ordering in the XAPI syntax. The XML syntax is
# safeguarded by using named parameters.
coordinates = "50.745,7.17,50.75,7.18"
osm_elements = {"way": []}

# Create Query -> Overpass API supports regular expressions!
query = generate_query(coordinates, **osm_elements)
print query

# Fetch OSM-data using overpy library
# -> Problem: Zu ways keine georef. nodes... => resolve.missing in overpy, teuer... :(
result = fetch_osm(query)

# Convert resulting overpy object as shapely geometries and collect in dictionary
# Wanna let the class constructor handle this...?
nodes, ways = {}, {}

for result_node in result.nodes:
    lonlat = [result_node.lon, result_node.lat]
    nodes[result_node.id] = Node(id=result_node.id,
                                 lonlat=lonlat,
                                 tags=result_node.tags)

for result_way in result.ways:
    ways[result_way.id] = Way.from_overpy_obj(result_way)

file = codecs.open('/Users/blubber/Documents/TEMP/test.osm', 'w',
                   encoding="utf8")
# dump(result, file)
# file.close()

# ## Alternative: http GET request => Returns formatted XML, WAY EASIER, doesn't
# # even need much python =>  Using urllib2
# import urllib2
# import shapely
#
# overpass_server = "http://overpass-api.de/api/interpreter?data="
# url = str(overpass_server + query)
# print url
#
# req = urllib2.urlopen(url)
# blubb = req.read()
# print blubb
#
# # Export files
# file = open("/Users/blubber/Documents/TEMP/test2.osm", 'w')
# file.write(blubb)
# file.close()

## ======================= TEMP WORKSPACE =====================================

## XML Parser
# Convert received OSM/XML-Code to various formats, e.g. ESRI-Shape or
# python shapely objects

import weakref
import pyproj
from shapely.geometry import Point, LineString

projection = pyproj.Proj(
    '+proj=tmerc +lat_0=0 +lon_0=15 +k=1'
    '+x_0=6500000 +y_0=0 +ellps=bessel +units=m +no_defs')

# class Store:
# instances = []
# def storage(self, name=None):
# self.__class__.instances.append(weakref.proxy(self))
# self.name = name
#
# def print_instances(self):
#         for instance in test_instances:
#             print instance




Point1 = Node(id=1, lonlat=[39.101, -94.584], tags={"name": "Kansas City"})
Point1.tags

# {'lat': '41.88', 'lon': '-87.63', 'name': 'Chicago'}
Point2 = Node(id=2, lonlat=[41.88, -87.63], tags={"name": "Chicago"})

nodes = {Point1.id: Point1, Point2.id: Point2}

Way1 = Way(id=1, nd_refs=nodes.keys(), tags={"highway": "ICE"})
ways = {Way1.id: Way1}
Way1.geometry.length

# Write something from shapefile
from shapely.geometry import mapping
import fiona

# Write something to shapefile
from shapely.geometry import mapping
import fiona

# Define geometry specs
schema_point = {'geometry': 'Point',
                'properties': {'name': 'str', 'lala': 'str'}}
with fiona.collection('/Users/blubber/Documents/TEMP/bla.shp', 'w',
                      'ESRI Shapefile', schema) as output:
    for node in nodes:
        output.write({
            'properties': {'name': 'point', 'lala': 'gubbel'},
            'geometry': mapping(nodes[node].geometry)
        })

schema_line = {'geometry': 'LineString',
               'properties': {'name': 'str', 'lala': 'str'}}
with fiona.collection('/Users/blubber/Documents/TEMP/bla.shp', 'w',
                      'ESRI Shapefile', schema) as output:
    for way in ways:
        output.write({
            'properties': {'name': 'Line', 'lala': 'gubbel'},
            'geometry': mapping(ways[way].geometry)
        })



