def generate_query(clip_to=None, verbosity="body", **osm_elements):
    # Todo
    # * Add support for ESRI-shp input as bbox
    """
    Function to generate OVERPASS API queries
    :param bbox: Supply bounding box in OverpassAPI format, currently only
    bbox supported
    :param verbosity: Output format ("body", "skeleton", "ids_only", "meta")
    :param osm_elements: dictionary of elements and associated tags
    :return:
    """

    def remove_whitespaces(str):
        return ''.join(str.split(' '))

    # Create bounding box

    # Initialize return query string
    return_query = str("(")

    if not osm_elements:
        print "Invalid Input, please provide one or more osm element types" \
              "to fetch (node, way, relation or map)"
        return_query = None
    else:
        for key in osm_elements.keys():
            return_query += str(key)
            if osm_elements[key] and not osm_elements[key] == "[]":
                return_query += str('[' + ']['.join(osm_elements[key]) + ']')
            if clip_to:
                return_query += str('(' + clip_to + ');')
        return_query += ");"
        # return_query += "out {verb};".format(verb=verbosity)
        return_query += "out;"

    return return_query