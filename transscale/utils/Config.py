from transscale.utils.DefaultValues import DefaultValues as Value, ConfigKeys as Key
from os.path import exists, join
from os import getcwd


# TODO add config file from where to read configurations
from transscale.utils.Logger import Logger


class Config:
    __config = {}

    def __init__(self, log: Logger, conf_path: str = None):
        self.__log = log
        self.__init_defaults()

        if conf_path is None or not exists(conf_path):
            self.__log.warning(f"[CONFIG] Config file [{conf_path}] not specified or not existing.\n"
                               f"\tUsing [{Value.System.CONF_PATH}] instead.")
            conf_path = join(getcwd(), Value.System.CONF_PATH)
        self.__read_config_file(conf_path)

    def __init_defaults(self):
        self.__config[Key.MONITORING_INTERVAL] = Value.System.Monitoring.interval
        self.__config[Key.MONITORING_WARMUP] = Value.System.Monitoring.warmup
        self.__config[Key.MAX_PAR] = Value.System.Environment.max_par
        self.__config[Key.MAX_TRANSP] = Value.System.Environment.max_transp

        self.__config[Key.SCALING_STRATEGY] = Value.Scaling.Strategy.module
        self.__config[Key.SCALING_OPTIMIZATION] = Value.Scaling.Strategy.optimization_method
        self.__config[Key.SCALING_METHOD] = Value.Scaling.Transprecision.method
        self.__config[Key.TPUT_THRESHOLD_TRANSP] = Value.Scaling.Transprecision.threshold
        self.__config[Key.TPUT_THRESHOLD_PAR] = Value.Scaling.Parallelism.threshold
        self.__config[Key.TPUT_THRESHOLD_COMBO] = Value.Scaling.Combined.threshold

        self.__config[Key.REDIS_HOME] = Value.Redis.home
        self.__config[Key.REDIS_DB] = Value.Redis.db_num
        self.__config[Key.REDIS_HOST] = Value.Redis.host
        self.__config[Key.REDIS_PORT] = Value.Redis.port

        self.__config[Key.FLINK_HOST] = Value.Flink.host
        self.__config[Key.FLINK_PORT] = Value.Flink.port
        self.__config[Key.FLINK_CMD] = Value.Flink.command
        self.__config[Key.FLINK_JOB_PATH] = Value.Flink.job_path

        self.__config[Key.DEBUG_LEVEL] = Value.System.Debug.level

    def get(self, key) -> any:
        if key in self.__config:
            return self.__config[key]

    def get_int(self, key) -> int:
        return int(self.get(key))

    def get_float(self, key) -> float:
        return float(self.get(key))

    def get_str(self, key):
        return str(self.get(key))

    def set(self, key, value):
        self.__config[key] = value

    def __read_config_file(self, conf_path: str):
        if exists(conf_path):

            import configparser as cp
            dummy_header = "config"

            parser = cp.ConfigParser()
            with open(conf_path) as cf:
                content = f"[{dummy_header}]\n" + cf.read()

            parser.read_string(content)
            conf = parser[dummy_header]

            for key in conf:
                if key in self.__config:
                    self.__config[key] = conf[key]
                else:
                    self.__log.error(f"[CONF] Specified key \"{key}\" does not exist")
        else:
            self.__log.warning(f"[CONF] No configuration file found")
