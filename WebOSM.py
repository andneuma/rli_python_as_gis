"""
Set of small tools for querying OSM web services
"""
import urllib
import xml.etree.ElementTree as ET  # XML-parsing

def fetch_admin_from_latlon(lat, lon):
    """
    Receive geo information from lat/lon point (reverse geocoding)
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