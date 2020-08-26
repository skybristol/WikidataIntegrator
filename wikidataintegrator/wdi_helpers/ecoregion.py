import datetime

from wikidataintegrator import wdi_core
from wikidataintegrator.wdi_helpers import id_mapper, try_write
from wikidataintegrator.wdi_helpers.wikibase_helper import WikibaseHelper


class Ecoregion(object):
    """
    Create an ecoregion item
    Example: Scattered High Ridges And Mountains

    An ecoregion item has the following required properties:
        title: **user_defined**
        description: **user_defined**
        instance of (P31): **user_defined** (wd_item)
        country (P17): **user_defined** (wd_item, one or more)
        located in the administrative or territorial entity (P131): **user_defined** (wd_item, one or more)
        coordinate location (P625): **user_defined** (str)
    Optional properties:
        part of (P361): (wd_item, one or more)
        has part (P527): (wd_item, one or more)

    Example usage:
    r = wdi_helpers.Ecoregion(
        title="Scattered High Ridges and Mountains",
        description="An EPA level 4 ecoregion",
        instance_of="Q52111338",
        country=["Q30"],
        located_in=["Q1649","Q1612"],
        coordinate_location='35.09,-94.01'
    )
    r.get_or_create(login)

    """
    # dict. key is a tuple of (sparql_endpoint_url, title), value is the qid of that release
    _release_cache = dict()

    def __init__(self, title, description, instance_of, country, located_in, coordinate_location,
                 part_of=None, has_part=None, mediawiki_api_url='https://www.wikidata.org/w/api.php',
                 sparql_endpoint_url='https://query.wikidata.org/sparql'):
        """

        :param title: title of ecoregion
        :type title: str
        :param description: description of ecoregion
        :type description: str
        :param instance_of: Wikidata qid that specifies the class of ecoregion
        :type instance_of: str
        :param country: Wikidata qids of country items the ecoregion intersects with
        :type country: list
        :param located_in: Wikidata qids of states/provinces and/or counties the ecoregion intersects with
        :type located_in: list
        :param coordinate_location: Coordinate string in lon,lat
        :type coordinate_location: str
        :param part_of: (optional) Wikidata qids of the 'parent' ecoregion(s) this ecoregion is a part of
        :type part_of: list
        :param has_part: (optional) Wikidata qids of the 'child' ecoregion that are part of this ecoregion
        :type has_part: list
        """
        self.title = title
        self.description = description
        self.instance_of = str(instance_of)
        self.country = country
        if isinstance(self.country, str):
            self.country = [self.country]
        self.located_in= located_in
        if isinstance(self.located_in, str):
            self.located_in = [self.located_in]
        self.coordinate_location = coordinate_location
        self.part_of = part_of
        if isinstance(self.part_of, str):
            self.part_of = [self.part_of]
        self.has_part = has_part
        if isinstance(self.has_part, str):
            self.has_part = [self.has_part]

        self.sparql_endpoint_url = sparql_endpoint_url
        self.mediawiki_api_url = mediawiki_api_url
        self.helper = WikibaseHelper(sparql_endpoint_url)

        self.statements = None

    def make_statements(self):
        s = []
        helper = self.helper

        # instance of appropriate ecoregion class
        s.append(wdi_core.WDItemID(helper.get_qid(self.instance_of), helper.get_pid("P31")))

        # countries ecoregion a part of
        for country_id in self.country:
            s.append(wdi_core.WDItemID(country_id, helper.get_pid("P17")))

        # states, provinces, counties ecoregion intersects with
        for location_id in self.located_in:
            s.append(wdi_core.WDString(location_id, helper.get_pid("P131")))

        # coordinate location of representative point
        s.append(wdi_core.WDItemID(helper.coordinate_mapper(self.coordinate_location), helper.get_pid("P625")))

        # Higher level classed ecoregions this ecoregion is a part of
        if self.part_of:
            for part_of_id in self.part_of:
                s.append(wdi_core.WDUrl(helper.get_qid(part_of_id), helper.get_pid("P361")))

        # Lower level classed ecoregions this ecoregion contains
        if self.has_part:
            for has_part_id in self.has_part:
                s.append(wdi_core.WDUrl(helper.get_qid(has_part_id), helper.get_pid("P527")))

        self.statements = s

    def get_or_create(self, login=None):

        # check in cache
        key = (self.sparql_endpoint_url, self.title)
        if key in self._release_cache:
            return self._release_cache[key]

        # check in wikidata
        # edition number, filter by edition of and instance of edition
        helper = self.helper
        edition_dict = id_mapper(helper.get_pid("P393"),
                                 ((helper.get_pid("P629"), self.edition_of_qid),
                                  (helper.get_pid("P31"), helper.get_qid("Q3331189"))),
                                 endpoint=self.sparql_endpoint_url)
        if edition_dict and self.edition in edition_dict:
            # add to cache
            self._release_cache[key] = edition_dict[self.edition]
            return edition_dict[self.edition]

        # create new
        if login is None:
            raise ValueError("login required to create item")

        self.make_statements()
        item = wdi_core.WDItemEngine(data=self.statements,
                                     mediawiki_api_url=self.mediawiki_api_url,
                                     sparql_endpoint_url=self.sparql_endpoint_url)
        item.set_label(self.title)
        item.set_description(description=self.description, lang='en')
        write_success = try_write(item, self.edition + "|" + self.edition_of_qid, 'P393|P629', login)
        if write_success:
            # add to cache
            self._release_cache[key] = item.wd_item_id
            return item.wd_item_id
        else:
            raise write_success

    def get_all_releases(self):
        # helper function to get all releases for the edition_of_qid given
        helper = self.helper
        edition_dict = id_mapper(helper.get_pid("P393"),
                                 ((helper.get_pid("P629"), self.edition_of_qid),
                                  (helper.get_pid("P31"), helper.get_qid("Q3331189"))),
                                 endpoint=self.sparql_endpoint_url)
        return edition_dict
