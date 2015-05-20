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
from overpy_helpers import *
import codecs

### WORKING SCRIPT

# Overpass API : Bounding box clauses always start with the lower latitude followed by lower longitude, then upper latitude then
# upper longitude. Note that this is different from the ordering in the XAPI syntax. The XML syntax is safeguarded by using named
# parameters.
coordinates = "50.746,7.154,50.748,7.157"
osm_elements = {"node": ['"amenity"="shop"', '"bla"="blubb"'],
                "way": []}

# Create Query -> Overpass API supports regular expressions!
query = generate_query(coordinates, **osm_elements)
print query

## Fetch and dump data to XML file
result = fetch_osm(query)

file = codecs.open('/Users/blubber/Documents/TEMP/test.osm', 'w', encoding="utf8")
dump(result, file)
file.close()

## Alternative: http GET request => Returns formatted XML, WAY EASIER, doesn't even need much python...
# Using urllib2
import urllib2

overpass_server = "http://overpass-api.de/api/interpreter?data="
url = str(overpass_server + query)
print url

req = urllib2.urlopen(url)
blubb = req.read()

file = open("/Users/blubber/Documents/TEMP/test2.osm", 'w')
file.write(blubb)
file.close()