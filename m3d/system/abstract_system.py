from m3d.config import config_service


class AbstractSystem(object):
    def __init__(self, config, source_system, database):

        # store parameters
        self.source_system = source_system
        self.database = database
        self.config = config

        # create config service
        self.config_service = config_service.ConfigService(config)
