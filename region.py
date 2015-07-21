import fiona  # handling ESRI shape format
from shapely.wkt import loads as loads
from shapely.geometry import Polygon, box
import weakref
import re

class Region:
    """
    Define region object, instances can be passed to Query() in order to set
    boundaries for queries in a PostGIS-DB
    """
    instances = {}

    def __init__(self, name=None, boundary=None):
        self.name = name
        self.__class__.instances[self.name] = weakref.proxy(self)

        self.set_boundaries(boundary)

    def set_boundaries(self, boundary):
        """
        Set boundary bbox
        :return:
        """
        if type(boundary) == str:
            # WKT-String formatted polygon
            if boundary.startswith("POLYGON\(\("):
                self.boundary_polygon = loads(boundary)
                self.bounds = self.boundary_polygon.bounds
                # store as wkt in order to maintain consistency
                self.boundary_polygon = self.boundary_polygon.wkt
            # Filepath to ESRI shape file
            elif boundary.endswith(".shp"):
                with fiona.open(boundary, 'r') as source:
                    # import as shapely polygon
                    self.boundary_polygon = Polygon(
                        next(source)['geometry']['coordinates'][0])
                self.bounds = self.boundary_polygon.bounds
                # store as wkt in order to maintain consistency
                self.boundary_polygon = self.boundary_polygon.wkt
            # Link to database table containing boundary polygon
            # format: [schema].[table]
            elif re.match(r"[a-zA-Z0-9]*[.]*[a-zA-Z0-9]", boundary):
                self.bounds = boundary
                self.boundary_polygon = None
            else:
                # print(
                #     "Warning: Wrong or unknown input format of boundary, returning NoneValue...")
                self.bounds = None
                self.boundary_polygon = None
        # tuple containing bbox, format: (xmin, ymin, xmax, ymax)
        elif type(boundary) == tuple:
            self.bounds = boundary
            self.boundary_polygon = box(*boundary)
            # store as wkt in order to maintain consistency
            self.boundary_polygon = self.boundary_polygon.wkt
        else:
            # print(
            #     "Warning: Wrong or unknown input format of boundary, returning NoneValue...")
            self.bounds = None
            self.boundary_polygon = None
