from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy, StrategyOptimization
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Logger import Logger


class ReconfigurationManager:

    def __init__(self, conf: Config, log: Logger):
        self.__log = log
        self.__debug = int(conf.get(Key.DEBUG_LEVEL))
        self.__log.debug(f"[RECONF_MNGR] Selected strategy is {conf.get(Key.SCALING_STRATEGY)}")
        self.__reconf_strategy = import_strategy(conf.get(Key.SCALING_STRATEGY), conf, log)
        self.__log.debug("[RECONF_MNGR] Strategy imported")
        self.__reconf_strategy.print_status()

    def get_scaleup_target(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        return self.__reconf_strategy.scale_up(possible_configurations, context)

    def get_scaledown_target(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        return self.__reconf_strategy.scale_down(possible_configurations, context)

    def get_scaling_optimization(self) -> StrategyOptimization:
        return self.__reconf_strategy.get_optimization()


def import_strategy(module_path: str, conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    from importlib import import_module

    module_path = module_path.rsplit('.', 1)
    module = import_module(f".{module_path[1]}", module_path[0])

    return module.init_strategy(conf, log)
