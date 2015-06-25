from postgis_query_helpers import *

## Example query using python PostGIS socket

main_dt = {}
main_dt['db_setup'] = {
    'db': "reiners_db",
    'host': '192.168.10.25',
    'port': 5432,
    'user': 'Andi',
    'pwd_filepath': '/Users/blubber/.db_connections.config'}

# main_dt['db_setup'] = {
# 'db': "osm",
# 'host': 'localhost',
# 'port': 5432,
#     'user': 'postgres_andi',
#     'pwd_filepath': '/Users/blubber/.db_connections.config'}

main_dt['query_features'] = {'schema': 'public',
                             'table': 'germany_polygon',
                             'geom_type': 'Polygon',
                             'geom_col': 'way',
                             'id_col': 'osm_id',
                             'select_cols': ['osm_id', 'name', 'amenity'],
                             'where_cond': "amenity='kindergarten'",
                             'SRID': 4326}

# Fetch geometries
result = fetch_geoms(main_dt,
                     boundary='/Users/blubber/Documents/TEMP/wustermark_extend.shp',
                     summary_table=True)


# Fetch admin boundary from lat/lon
res = fetch_admin_from_latlon(lat=51., lon=12.12)
for e in res:
    print e, ":", res[e]