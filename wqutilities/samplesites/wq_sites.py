import csv

import logging.config
from .sample_sites import SamplingSites
from .station_geometry import StationGeometry, GeometryList
from shapely.wkt import loads as wkt_loads
from shapely.geometry import mapping

logger = logging.getLogger(__name__)


class WQSite(StationGeometry):
    def __init__(self, **kwargs):
        StationGeometry.__init__(self, kwargs['name'], kwargs['wkt'])
        self.epa_id = kwargs.get('epa_id', "")
        self.description = kwargs.get('description', "")
        self.county = kwargs.get('county', "")
        self.extents_geometry = None
        if 'extentswkt' in kwargs:
            if kwargs['extentswkt'] is not None:
                if "MULTILINESTRING" in kwargs['extentswkt']:
                    self.extents_geometry = wkt_loads(kwargs['extentswkt'])
        return

    def get_extents_coords(self):
        coords = None
        if self.extents_geometry is not None:
            mapped = mapping(self.extents_geometry)
            coords = mapped['coordinates']
        return coords


"""
Overrides the default sampling_sites object so we can load the sites from the data.
"""


class WQSampleSites(SamplingSites):
    """
    Function: load_sites
    Purpose: Given the file_name in the kwargs, this will read the file and load up the sampling
      sites we are working with.
    Parameters:
      **kwargs - Must have file_name which is full path to the sampling sites csv file.
    Return:
      True if successfully loaded, otherwise False.
    """

    def load_sites(self, **kwargs):
        wq_boundaries = None
        if 'file_name' in kwargs:
            if 'boundary_file' in kwargs:
                wq_boundaries = GeometryList()
                wq_boundaries.load(kwargs['boundary_file'])

            try:
                header_row = ["WKT", "EPAbeachID", "SPLocation", "Description", "County", "Boundary", "ExtentsWKT"]
                logger.debug(f"Reading sample sites file: {kwargs['file_name']}")

                sites_file = open(kwargs['file_name'], "r")
                dict_file = csv.DictReader(sites_file, delimiter=',', quotechar='"', fieldnames=header_row)
            except IOError as e:
                logger.exception(e)
            else:
                line_num = 0
                for row in dict_file:
                    if line_num > 0:
                        add_site = False
                        # The site could be in multiple boundaries, so let's search to see if it is.
                        station = self.get_site(row['SPLocation'])
                        if station is None:
                            add_site = True
                            extents_wkt = None
                            if 'ExtentsWKT' in row:
                                extents_wkt = row['ExtentsWKT']
                            station = WQSite(name=row['SPLocation'],
                                             wkt=row['WKT'],
                                             epa_id=row['EPAbeachID'],
                                             description=row['Description'],
                                             county=row['County'],
                                             extentswkt=extents_wkt)
                            logger.debug(f"Processing sample site: {row['SPLocation']}")
                            self.append(station)
                            try:
                                if len(row['Boundary']):
                                    boundaries = row['Boundary'].split(',')
                                    for boundary in boundaries:
                                        logger.debug("Sample site: {row['SPLocation']} Boundary: {boundary}")
                                        boundary_geometry = wq_boundaries.get_geometry_item(boundary)
                                        if add_site:
                                            # Add the containing boundary
                                            station.contained_by.append(boundary_geometry)
                            except AttributeError as e:
                                logger.exception(e)
                    line_num += 1
                return True
        return False

    def add_site(self, site: WQSite):
        if self.get_site(site.name) is None:
            self.append(site)

