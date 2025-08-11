import os
import logging.config
import time
import json
import geojson

def contains(list, filter):
  for x in list:
    if filter(x):
      return True
  return False

class WQSampleData:
  def __init__(self, **kwargs):
    self._station = kwargs.get('station', None)
    self._date_time = kwargs.get('date_time', None)
    self._value = kwargs.get('value', None)

  @property
  def station(self):
    return self._station
  @station.setter
  def station(self, station):
    self._station = station

  @property
  def date_time(self):
    return self._date_time
  @date_time.setter
  def date_time(self, date_time):
    self._date_time = date_time

  @property
  def value(self):
    return self._value
  @value.setter
  def value(self, value):
    self._value = value

class WQSamplesCollection:
  def __init__(self):
    self._wq_samples = {}

  def __len__(self):
    return len(self._wq_samples.keys())

  def append(self, wq_sample):
    if type(wq_sample) is list:
      for sample in wq_sample:
        if sample.station not in self._wq_samples:
          self._wq_samples[wq_sample.station] = []
        self._wq_samples[wq_sample.station].append(sample)
    else:
      if wq_sample.station not in self._wq_samples:
        self._wq_samples[wq_sample.station] = []
      self._wq_samples[wq_sample.station].append(wq_sample)

  def __getitem__(self, name):
      return self._wq_samples[name]

  def __iter__(self):
      return iter(self._wq_samples)

  def keys(self):
      return self._wq_samples.keys()

  def items(self):
      return self._wq_samples.items()

class WQAdvisoriesFile:
  def __init__(self, sample_sites):
    self.logger = logging.getLogger(self.__class__.__name__)
    self.sample_sites = sample_sites

  def create_file(self, out_file_name, wq_samples):
    try:
      current_sample_sites = wq_samples.keys()
      with open(out_file_name, 'r') as station_json_file:
        json_data = json.loads(station_json_file.read())
        if 'features' in json_data:
          for site in self.sample_sites:
            self.logger.debug("Searching for site: %s in json data" % (site.name))
            site_found = False
            features = json_data['features']
            for feature in features:
              properties = feature['properties']
              station = properties['station']
              if site.name == station:
                self.logger.debug("Found site: %s" % (site.name))
                site_found = True
                site_nfo = site
                """
                for site in self.sample_sites:
                  if site.name == station:
                    site_nfo = site
                    break
                """
                if station in current_sample_sites:
                  self.logger.debug("Adding data for site: %s" % (site.name))
                  wq_samples[station].sort(key=lambda x: x.date_time, reverse=False)

                  if 'test' in properties:
                    properties['test']['beachadvisories'] = {
                      'date': wq_samples[station][-1].date_time.strftime('%Y-%m-%d %H:%M:%S'),
                      'station': station,
                      'value': wq_samples[station][-1].value
                    }

                break
            if site_nfo is not None:
              #Update the description fields since we might modify them
              #from time to time, otherwise have to hand edit the json files.
              properties['locale'] = site_nfo.description
              properties['desc'] = site_nfo.description
              if site_nfo is not None and site_nfo.extents_geometry is not None:
                self.logger.debug("Adding extents for site: %s" % (site.name))
                extents_json = geojson.Feature(geometry=site.extents_geometry, properties={})
                feature['properties']['extents_geometry'] = extents_json

            if not site_found:
              self.logger.debug("Site: %s not found, building feature" % (site.name))
              feature = self.build_feature(site, "", [])
              features.append(feature)
        else:
          self.logger.debug("Features not found in json data, building")
          features = self.build_site_features(wq_samples)
    except (IOError, Exception) as e:
      self.logger.error("File: %s does not exist yet." % (out_file_name))
      features = self.build_site_features(wq_samples)
    try:
      with open(out_file_name, "w") as out_file_obj:
        #features = self.build_site_features(wq_samples)
        json_data = {
          'type': 'FeatureCollection',
          'features': features
        }
        self.logger.debug("Writing json file: %s" % (out_file_name))

        out_file_obj.write(json.dumps(json_data, sort_keys=True))
    except (IOError, Exception) as e:
      self.logger.exception(e)

    return

  def build_feature(self, site, sample_date, values):
    beachadvisories = {
      'date': '',
      'station': site.name,
      'value': ''
    }
    if len(values):
      beachadvisories = {
        'date': sample_date,
        'station': site.name,
        'value': values
      }
    feature = {
      'type': 'Feature',
      'geometry': {
        'type': 'Point',
        'coordinates': [site.object_geometry.x, site.object_geometry.y]
      },
      'properties': {
        'locale': site.description,
        'sign': False,
        'station': site.name,
        'epaid': site.epa_id,
        'beach': site.county,
        'desc': site.description,
        'len': '',
        'test': {
          'beachadvisories': beachadvisories
        }
      }
    }
    extents_json = None
    if site.extents_geometry is not None:
      extents_json = geojson.Feature(geometry=site.extents_geometry, properties={})
      feature['properties']['extents_geometry'] = extents_json

    return feature

  def build_site_features(self, wq_samples):
    start_time = time.time()
    self.logger.debug("Starting build_feature_logger")
    #Sort the data based on the date time of the sample(s).
    for site in wq_samples:
      wq_samples[site].sort(key=lambda x: x.date_time, reverse=False)

    features = []
    for site in self.sample_sites:
      bacteria_data = {}
      if site.name in wq_samples:
        site_data = wq_samples[site.name]
        bacteria_data = site_data[-1]
        feature = self.build_feature(site, bacteria_data.date_time.strftime('%Y-%m-%d %H:%M:%S'), [bacteria_data.value])
      else:
        feature = self.build_feature(site, "", [])

      self.logger.debug("Adding feature site: %s Desc: %s" % (site.name, site.description))
      features.append(feature)
    self.logger.debug("Finished build_feature_logger in %f seconds" % (time.time()-start_time))
    return features


class WQStationAdvisoriesFile:
  def __init__(self, sample_site):
    self.logger = logging.getLogger(self.__class__.__name__)
    self.sample_site = sample_site

  def create_file(self, out_file_directory, wq_samples):
    start_time = time.time()
    self.logger.debug("Starting create_file")
    station_filename = os.path.join(out_file_directory, "%s.json" % (self.sample_site.name))
    beach_advisories = []
    if self.sample_site.name in wq_samples:
      samples = wq_samples[self.sample_site.name]
      for sample in samples:
        beach_advisories.append({
          'date': sample.date_time.strftime('%Y-%m-%d %H:%M:%S'),
          'station': self.sample_site.name,
          'value': [sample.value]
        })
    if os.path.isfile(station_filename) and os.stat(station_filename).st_size > 0:
      try:
        self.logger.debug("Opening station JSON file: %s" % (station_filename))
        with open(station_filename, 'r') as station_json_file:
          feature = json.loads(station_json_file.read())
          if feature is not None:
            if 'test' in feature['properties']:
              file_beachadvisories = feature['properties']['test']['beachadvisories']
            else:
              file_beachadvisories = []
            # Make sure the date is not already in the list.
            for test_data in beach_advisories:
              if not contains(file_beachadvisories, lambda x: x['date'] == test_data['date']):
                self.logger.debug("Station: %s adding date: %s" % (self.sample_site.name, test_data['date']))
                file_beachadvisories.append(test_data)
                file_beachadvisories.sort(key=lambda x: x['date'], reverse=False)
      except (IOError, Exception) as e:
        if self.logger:
          self.logger.exception(e)
    else:
      self.logger.debug("Creating new station JSON file for: %s" % (self.sample_site.name))

      feature = {
        'type': 'Feature',
        'geometry': {
          'type': 'Point',
          'coordinates': [self.sample_site.object_geometry.x, self.sample_site.object_geometry.y]
        },
        'properties': {
          'locale': self.sample_site.description,
          'sign': False,
          'station': self.sample_site.name,
          'epaid': self.sample_site.epa_id,
          'beach': self.sample_site.county,
          'desc': self.sample_site.description,
          'len': '',
          'test': {
            'beachadvisories': beach_advisories
          }
        }
      }
      extents_json = None
      if self.sample_site.extents_geometry is not None:
        extents_json = geojson.Feature(geometry=self.sample_site.extents_geometry, properties={})
        feature['properties']['extents_geometry'] = extents_json
    try:
      if feature is not None:
        self.logger.debug("Creating file: %s" % (station_filename))
        with open(station_filename, 'w') as station_json_file:
          feature_json = json.dumps(feature)
          #self.logger.debug("Feature: %s" % (feature_json))
          station_json_file.write(feature_json)
      else:
        self.logger.error("Feature is None")
    except (IOError, Exception) as e:
      self.logger.exception(e)

    self.logger.debug("Finished create_file in %f seconds" % (time.time() - start_time))
