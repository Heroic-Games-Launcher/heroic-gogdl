# Manages authorization
# with ability to have multiple tokens (will come in handy in the future)
import json
import logging
import os.path
import requests
import time
from gogdl import version

CODE_URL = "https://auth.gog.com/token?client_id=46899977096215655&client_secret=9d85c43b1482497dbbce61f6e4aa173a433796eeae2ca8c5f6129f2dc4de46d9&grant_type=authorization_code&redirect_uri=https%3A%2F%2Fembed.gog.com%2Fon_login_success%3Forigin%3Dclient&code="
CLIENT_ID = "46899977096215655"
CLIENT_SECRET = "9d85c43b1482497dbbce61f6e4aa173a433796eeae2ca8c5f6129f2dc4de46d9"


class AuthorizationManager:
    def __init__(self, config_path):
        self.session = requests.session()
        self.logger = logging.getLogger("AUTH")

        self.config_path = config_path
        self.credentials_data = {}
        self.__read_config()

        self.session.headers.update({
            'User-Agent': f'gogdl/{version} (Heroic Games Launcher)'
        })

    def __read_config(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                self.credentials_data = json.loads(f.read())
                f.close()

    def __write_config(self):
        with open(self.config_path, "w") as f:
            f.write(json.dumps(self.credentials_data))
            f.close()

    def get_credentials(self, client_id=None, client_secret=None):
        """
        Reads data from config and returns it
        :param client_id:
        :return: dict with credentials or None if not present
        """
        if not client_id:
            client_id = CLIENT_ID

        if not client_secret:
            client_secret = CLIENT_SECRET

        credentials = self.credentials_data.get(client_id)
        if not credentials:
            return None

        if self.is_credential_expired(client_id):
            if self.refresh_credentials(client_id, client_secret):
                credentials = self.credentials_data.get(client_id)
            else:
                raise Exception("Failed to obtain credentials")
        return credentials

    def is_credential_expired(self, client_id=None) -> bool:
        """
        Checks if provided client_id credential is expired
        If the credential with this client_id doesn't exist raises Exception
        :param client_id:
        :return: whether credentials are expired
        """
        if not client_id:
            client_id = CLIENT_ID
        credentials = self.credentials_data.get(client_id)

        if not credentials:
            raise ValueError("Credential doesn't exist")

        return time.time() >= credentials['loginTime'] + credentials["expires_in"]

    def refresh_credentials(self, client_id=None, client_secret=None) -> bool:
        """
        Refreshes credentials and saves them to config
        Can be used to obtain credentials for game scopes
        :param client_id:
        :param client_secret:
        :return: bool if operation was success
        """
        if not client_id:
            client_id = CLIENT_ID
        if not client_secret:
            client_secret = CLIENT_SECRET

        credentials = self.credentials_data.get(CLIENT_ID)
        refresh_token = credentials["refresh_token"]

        url = f"https://auth.gog.com/token?client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token&refresh_token={refresh_token}"

        response = self.session.get(url)

        if not response.ok:
            return False
        data = response.json()
        data["loginTime"] = time.time()
        self.credentials_data.update({client_id: data})
        self.__write_config()
        return True

    def handle_cli(self, arguments, unknown_arguments):
        self.logger.debug("Handling cli")

        if arguments.authorization_code:
            response = self.session.get(CODE_URL + arguments.authorization_code)

            if not response.ok:
                print(json.dumps({"error": True}))
                return
            data = response.json()
            data.update({"loginTime": time.time()})

            self.credentials_data.update({CLIENT_ID: data})
            self.__write_config()

            print(json.dumps(data))
            return

        client_id = arguments.client_id or CLIENT_ID
        client_secret = arguments.client_secret or CLIENT_SECRET

        credentials = self.get_credentials(client_id)

        print(json.dumps(credentials))
