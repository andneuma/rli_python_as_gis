from postgis_query_helpers import *

## Example query using python PostGIS socket

def main():
    # Define db setup and query features
    main_dt = {}

    main_dt['db_setup'] = {
        'db': "osm",
        'host': 'localhost',
        'user': 'postgres_andi',
        'password': None}

    main_dt['query_features'] = {
        'schema': 'public',
        'table': 'brandenburg_polygon',
        'geom_type': 'Polygon',
        'geom_col': 'way',
        'id_col': 'osm_id',
        'select_cols': ['osm_id', 'name', 'amenity'],
        'where_cond': "building='yes' AND amenity='kindergarten'",
        'SRID': 4326}

    # Fetch geometries
    result = fetch_geoms(main_dt,
                         boundary='/Users/blubber/Documents/TEMP/wustermark_extend.shp',
                         summary_table=True)

    # Save results to harddisk
    view2shp(result, '/Users/blubber/Documents/TEMP/bb_roads.shp')


if __name__ == "__main__":
    main()