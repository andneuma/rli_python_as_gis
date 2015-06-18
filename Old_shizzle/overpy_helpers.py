import overpy

## Fetch data
def fetch_osm(query):
    """
    :param query: Supply Overpass API query string
    (see https://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide)
    :return:
    """
    api = overpy.Overpass()
    result = api.query(query)

    print "Fetched {nodes} nodes, {ways} ways and {rels}" \
          "relations \n\nquery: '{query}'".format(
        nodes=len(result.nodes),
        ways=len(result.ways),
        rels=len(result.relations),
        query=query)
    return result


## Dump results to XML
# Todo
# * Determine bbox in case of nodes == False

def dump(result, fp):
    """
    :param result: Overpy-object
    :param fp: filepath-string
    :return:
    """
    # Write to supplied file object
    fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    fp.write('<osm version="0.6" generator="OverPy {0}">\n'.format(
        overpy.__version__))

    # Determine bbox ?
    # Problem : No nodes, no bbox... FIX THIS
    lat_min = result.nodes[0].lat
    lat_max = lat_min
    lon_min = result.nodes[0].lon
    lon_max = lon_min
    for node in result.nodes:
        if node.lat < lat_min:
            lat_min = node.lat
        elif node.lat > lat_max:
            lat_max = node.lat

        if node.lon < lon_min:
            lon_min = node.lon
        elif node.lon > lon_max:
            lon_max = node.lon

    fp.write(
        '<bounds minlat="{0:f}" minlon="{1:f}" maxlat="{2:f}" maxlon="{3:f}"/>\n'.format(
            lat_min,
            lat_max,
            lon_min,
            lon_max
        )
    )

    # Write node data
    for node in result.nodes:
        fp.write(
            '<node id="{0:d}" lat="{1:f}" lon="{2:f}"'.format(
                node.id,
                node.lat,
                node.lon
            )
        )
        if len(node.tags) == 0:
            fp.write('/>\n')
            continue
        fp.write('>\n')
        for k, v in node.tags.items():
            fp.write(
                u'<tag k="{0:s}" v="{1:s}"/>\n'.format(k, v))
        fp.write('</node>\n')

    # Write way data
    for way in result.ways:
        fp.write('<way id="{0:d}"'.format(node.id))
        if len(way.nodes) == 0 and len(way.tags) == 0:
            fp.write('/>\n')
            continue
        fp.write('>\n')
        for node in way.nodes:
            fp.write('<nd ref="{0:d}"/>\n'.format(node.id))

        for k, v in way.tags.items():
            fp.write(
                u'<tag k="{0:s}" v="{1:s}"/>\n'.format(k, v))
        fp.write('</way>\n')

    # Write relation data
    for relation in result.relations:
        fp.write(u'<relation id="{0:d}'.format(relation.id))
        if len(relation.tags) == 0 and len(relation.members) == 0:
            fp.write('/>\n')

        for member in relation.members:
            if not isinstance(member, overpy.RelationMember):
                continue
            fp.write(
                u'<member type="{0:s}" ref="{1:d}" role="{2:s}"/>\n'.format(
                    member._type_value,
                    member.ref,
                    member.role
                )
            )

        for k, v in relation.tags.items():
            fp.write(
                u'<tag k="{0:s}" v="{1:s}"/>\n'.format(k, v))
        fp.write('</relation>\n')

    fp.write('</osm>')