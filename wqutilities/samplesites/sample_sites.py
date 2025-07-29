class SamplingSites(list):
  def load_sites(self, **kwargs):
    return False

  def get_site(self, site_name):
    for site in self:
      if site.name.lower() == site_name.lower():
        return site
    return None
