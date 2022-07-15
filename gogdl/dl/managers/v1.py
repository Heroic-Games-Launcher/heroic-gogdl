# Handle old games downloading via V1 depot system
# V1 is there since GOG 1.0 days, it has no compression and relies on downloading chunks from big main.bin file


class Manager:
    def __init__(self, generic_manager):
        self.game_id = generic_manager.game_id
        self.arguments = generic_manager.arguments
        self.unknown_arguments = generic_manager.unknown_arguments

        self.api_handler = generic_manager.api_handler

        self.build = generic_manager.target_build
