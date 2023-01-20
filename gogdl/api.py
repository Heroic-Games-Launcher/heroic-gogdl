import logging
import time
import requests
import json
from multiprocessing import cpu_count
from gogdl.dl import dl_utils
import gogdl.constants as constants


class ApiHandler:
    def __init__(self, token):
        self.logger = logging.getLogger("API")
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=cpu_count())
        self.session.mount("https://", adapter)
        self.session.headers = {
            'User-Agent': 'GOGGalaxyClient/2.0.45.61 (GOG Galaxy)'
        }
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        self.owned = []

        self.endpoints = dict()  # Map of secure link endpoints
        self.working_on_ids = list()  # List of products we are waiting for to complete getting the secure link

    def get_item_data(self, id, expanded=[]):
        self.logger.info(f"Getting info from products endpoint for id: {id}")
        url = f'{constants.GOG_API}/products/{id}'
        expanded_arg = '?expand='
        if len(expanded) > 0:
            expanded_arg += ','.join(expanded)
            url += expanded_arg
        response = self.session.get(url)
        self.logger.debug(url)
        if response.ok:
            return response.json()
        else:
            self.logger.error(f"Request failed {response}")

    def get_game_details(self, id):
        url = f'{constants.GOG_EMBED}/account/gameDetails/{id}.json'
        response = self.session.get(url)
        self.logger.debug(url)
        if response.ok:
            return response.json()

    def get_dependenices_list(self, depot_version=2):
        self.logger.info("Getting Dependencies repository")
        url = constants.DEPENDENCIES_URL if depot_version == 2 else constants.DEPENDENCIES_V1_URL
        response = self.session.get(url)
        if not response.ok:
            return None

        json_data = json.loads(response.content)
        if 'repository_manifest' in json_data:
            self.logger.info("Getting repository manifest")
            return dl_utils.get_zlib_encoded(self, str(json_data['repository_manifest']))[0], json_data.get('version')

    def does_user_own(self, id):
        if not self.owned:
            response = self.session.get(f'{constants.GOG_EMBED}/user/data/games')
            self.owned = response.json()['owned']
        for owned in self.owned:
            if str(owned) == str(id):
                return True
        return False

    def __obtain_secure_link(self, id):
        self.endpoints[id] = None
        return dl_utils.get_secure_link(self, '/', id)

    def get_new_secure_link(self, id):
        if id not in self.working_on_ids:
            self.working_on_ids.append(id)
            new = self.__obtain_secure_link(id)
            self.endpoints[id] = new
            self.working_on_ids.remove(id)
            return new
        else:
            while True:
                if self.endpoints.get(id):
                    return self.endpoints[id]

    def get_secure_link(self, id):
        if self.endpoints.get(id):
            return self.endpoints.get(id)
        else:
            while True:  # Await for other thread to fetch the data
                if self.endpoints.get(id):
                    return self.endpoints.get(id)

                time.sleep(0.2)
