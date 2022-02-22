import logging
import requests
import json
from multiprocessing import cpu_count
from gogdl.dl.dl_utils import get_zlib_encoded
import gogdl.constants as constants

class ApiHandler():
    def __init__(self, token):
        self.logger = logging.getLogger("API")
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=cpu_count())
        self.session.mount("https://", adapter)
        self.session.headers = {
            "Authorization": f"Bearer {token}",
            'User-Agent': 'GOGGalaxyClient/2.0.45.61 (GOG Galaxy)'
        }
        self.owned = []

    def get_item_data(self, id, expanded=[]):
        url = f'{constants.GOG_API}/products/{id}'
        expanded_arg = '?expand='
        if(len(expanded) > 0):
            expanded_arg += ','.join(expanded)
            url += expanded_arg
        response = self.session.get(url)
        self.logger.debug(url)
        if response.ok:
            return response.json()    

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
            return get_zlib_encoded(self, str(json_data['repository_manifest']))[0], json_data.get('version')

    def does_user_own(self, id):
        if self.owned == []:
            response = self.session.get(f'{constants.GOG_EMBED}/user/data/games')
            self.owned = response.json()['owned']
        for owned in self.owned:
            if str(owned) == str(id):
                return True
        return False