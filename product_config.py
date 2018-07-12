import configparser

class ProductConfig:
    
    _config_section_name = 'database'

    _host = ''
    @property
    def host(self) -> str:
        return self._host
    
    _port = ''
    @property
    def port(self) -> str:
        return self._port

    _password= ''
    @property
    def password(self) -> str:
        return self._password
    
    _user = ''
    @property
    def user(self) -> str:
        return self._user

    _database_name = ''
    @property
    def database_name(self) -> str:
        return self._database_name

    _charaset = ''
    @property
    def charaset(self) -> str:
        return self._charaset

    def load_inifile(self, file_path:str, encoding ='UTF-8'):
        config = configparser.ConfigParser()
        config.read(file_path,encoding)
        self.dump_config(config)
        self.parse(config)


    def dump_config(self, config:configparser.ConfigParser):
        for section in config.sections():
            for option in config.options(section):
                print('section:%s option:%s value:%s' % (section,option,config.get(section,option)))

    
    def parse(self,config:configparser.ConfigParser):
        options = config[self._config_section_name]
        self._host = options['host']
        self._port = options['port']
        self._user = options['user']
        self._password = options['passwd']
        self._database_name = options['db']
        self._charaset = options['charaset']

if __name__ == '__main__':
    config = ProductConfig()
    config.load_inifile('config.ini')

        






