from shapely.wkt import loads as wkt_loads
import csv
import logging.config

logger = logging.getLogger(__name__)

"""
item_geometry
COntains name of boundary and the geometry that localizes it, usually a WKT POlygon.
"""


class ItemGeometry:
    def __init__(self, name, wkt=None):
        self.name = name  # Name for the object
        if wkt is not None:
            self.object_geometry = wkt_loads(wkt)  # Shapely object


"""
station_geometry
COntains name of sample site and the geometry that localizes it, usually a WKT Point.
"""


class StationGeometry(ItemGeometry):
    def __init__(self, name, wkt=None):
        ItemGeometry.__init__(self, name, wkt)
        self.contained_by = []  # THe boundaries that the station resides in.

    def add_boundary(self, name, wkt):
        self.contained_by.append(ItemGeometry(name, wkt))


"""
geometry_list
Class that loads simple CSV file containing the WKT and NAME of polygon boundaries.
"""


class GeometryList(list):
    """
    Function: load
    Purpose: Loads the given CSV file, file_name, and creates a list of the boundary objects.
    Parameters:
      file_name = full path to the CSV to load. CSV file must have WKT column and NAME column.
    Return: True if successully loaded, otherwise False.
    """

    def load(self, file_name):
        header_row = ["WKT", "NAME"]
        try:
            geometry_file = open(file_name, "r")
        except (IOError, Exception) as e:
            logger.exception(e)
        else:
            line_num = 0
            try:
                logger.debug(f"Open boundary file: {file_name}")
                dict_file = csv.DictReader(geometry_file, delimiter=',', quotechar='"', fieldnames=header_row)

                for row in dict_file:
                    if line_num > 0:
                        logger.debug(f"Building boundary polygon for: {row['NAME']}")
                        self.append(ItemGeometry(row['NAME'], row['WKT']))
                    line_num += 1

                return True

            except (IOError, Exception) as e:
                logger.error(f"Geometry creation issue on line: {line_num}")
                logger.exception(e)

        return False

    def get_geometry_item(self, name):
        for geometry_item in self:
            if geometry_item.name.lower() == name.lower():
                return geometry_item
        return None
