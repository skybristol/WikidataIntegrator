from wikidataintegrator import wdi_core
from wikidataintegrator.wdi_helpers import try_write, items_by_label, PROPS
import wikidataintegrator.wdi_helpers as wdh
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
        latitude (part of P625): **user_defined** (str)
        longitude (part of P625): **user_defined** (str)
    Optional properties:
        part of (P361): (wd_item, one or more)
        has part (P527): (wd_item, one or more)

    Example usage:
    r = wdi_helpers.Ecoregion(
        title="Scattered High Ridges and Mountains",
        instance_of="US_Eco_Level3",
        country=["Q30"],
        located_in=["Q1649","Q1612"],
        latitude=35.09
        longitude=-94.01
    )
    r.get_or_create(login)

    """
    # dict. key is a tuple of (sparql_endpoint_url, title), value is the qid of that release
    _release_cache = dict()

    def __init__(self, identifier, title, instance_of, country, located_in, latitude, longitude,
                 part_of=None, has_part=None,
                 mediawiki_api_url='https://www.wikidata.org/w/api.php',
                 sparql_endpoint_url='https://query.wikidata.org/sparql'):
        """

        :param identifier: Contextual identifier for the item that is unique in context and is created in a form that
        can either be used to identify the appropriate external ID property from Wikidata or be used as an identifier
        via an alt label
        :type identifier: str
        :param title: title of ecoregion
        :type title: str
        :param instance_of: Name alias to the Wikidata item that classes the ecoregion
        :type instance_of: str
        :param country: Wikidata qids of country items the ecoregion intersects with
        :type country: list
        :param located_in: Wikidata qids of states/provinces and/or counties the ecoregion intersects with
        :type located_in: list
        :param latitude: Latitude coordinate of representational point
        :type latitude: float
        :param longitude: Longitude coordinate of representational point
        :type longitude: float
        :param part_of: (optional) Wikidata qids of the 'parent' ecoregion(s) this ecoregion is a part of
        :type part_of: list
        :param has_part: (optional) Wikidata qids of the 'child' ecoregion that are part of this ecoregion
        :type has_part: list
        """
        self.identifier = self.validate_identifier(identifier)
        self.title = str(title)
        self.instance_of = str(instance_of)
        self.country = country
        if isinstance(self.country, str):
            self.country = [self.country]
        self.located_in= located_in
        if isinstance(self.located_in, str):
            self.located_in = [self.located_in]
        self.latitude = float(latitude)
        self.longitude = float(longitude)
        self.part_of = part_of
        if isinstance(self.part_of, str):
            self.part_of = [self.part_of]
        self.has_part = has_part
        if isinstance(self.has_part, str):
            self.has_part = [self.has_part]

        self.data_source_url = "https://www.epa.gov/eco-research/ecoregions"
        self.code_source_url = "https://github.com/skybristol/wdbots"

        self.classification_item = "Q52111282"
        self.id_prefixes = ["NA_L1CODE","NA_L2CODE","NA_L3CODE","US_L3CODE","US_L4CODE"]

        self.sparql_endpoint_url = sparql_endpoint_url
        self.mediawiki_api_url = mediawiki_api_url
        self.helper = WikibaseHelper(sparql_endpoint_url)

        # Use the known alias for ecoregion classification to retrieve the associated Wikidata item
        # Set description based on ecoregion class label and qid value for classification of item
        instance_of_record = wdh.items_by_label(
            search_property=PROPS['subclass of'],
            search_subject=self.classification_item,
            label=self.instance_of,
            return_raw_data=True
        )
        self.description = f"an instance of {instance_of_record['source_data']['itemLabel']['value']}"
        self.instance_of_qid = instance_of_record["wdid"]

        self.statements = None

    def validate_identifier(self, identifier):
        """
        Stub to contain functionality for processing identifiers for appropriate placement in items.
        :param identifier:
        :return:
        """
        return str(identifier)

    def make_statements(self):
        s = []

        data_ref = wdi_core.WDUrl(
            self.data_source_url,
            PROPS['reference URL'],
            is_reference=True
        )
        code_ref = wdi_core.WDUrl(
            self.data_source_url,
            PROPS['source repo'],
            is_reference=True
        )
        references = [data_ref, code_ref]

        s.append(
            wdi_core.WDItemID(
                self.instance_of_qid,
                PROPS['instance of'],
                references=references
            )
        )

        # countries ecoregion a part of
        for country_id in self.country:
            s.append(
                wdi_core.WDItemID(
                    country_id,
                    PROPS['country'],
                    references=references
                )
            )

        # states, provinces, counties ecoregion intersects with
        for location_id in self.located_in:
            s.append(
                wdi_core.WDItemID(
                    location_id,
                    PROPS['locality'],
                    references=references
                )
            )

        # coordinate location of representative point
        s.append(wdi_core.WDGlobeCoordinate(
            latitude=self.latitude,
            longitude=self.longitude,
            precision=4.25,
            prop_nr=PROPS['coordinate location'],
            references=references
        ))

        # Higher level classed ecoregions this ecoregion is a part of
        if self.part_of:
            for part_of_id in self.part_of:
                s.append(
                    wdi_core.WDItemID(
                        part_of_id,
                        PROPS['part of'],
                        references=references
                    )
                )

        # Lower level classed ecoregions this ecoregion contains
        if self.has_part:
            for has_part_id in self.has_part:
                s.append(
                    wdi_core.WDItemID(
                        has_part_id,
                        PROPS['has part'],
                        references=references
                    )
                )

        self.statements = s

    def get_or_create(self, login=None):

        # check in cache
        key = (self.sparql_endpoint_url, self.identifier)
        if key in self._release_cache:
            return self._release_cache[key]

        # check in wikidata
        # alternate label matching id pattern
        id_map = items_by_label(
            search_property=PROPS['instance of'],
            search_subject=self.instance_of_qid,
            return_raw_data=True,
            label=self.identifier,
            id_prefixes=self.id_prefixes
        )

        if id_map is not None and "id_mapper" in id_map.keys():
            # add to cache
            self._release_cache[key] = id_map["id_mapper"][self.identifier]
            return id_map[self.identifier]

        # create new
        if login is None:
            raise ValueError("login required to create item")

        self.make_statements()
        item = wdi_core.WDItemEngine(data=self.statements,
                                     mediawiki_api_url=self.mediawiki_api_url,
                                     sparql_endpoint_url=self.sparql_endpoint_url)
        item.set_label(self.title)
        item.set_aliases([self.identifier])
        item.set_description(description=self.description, lang='en')

        write_success = try_write(
            item,
            record_id=self.identifier,
            record_prop="P0",
            login=login
        )
        if write_success:
            # add to cache
            self._release_cache[key] = item.wd_item_id
            return item.wd_item_id
        else:
            raise write_success

