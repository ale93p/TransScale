from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key
from transscale.components.RuntimeContext import RuntimeContext


class ReconfigurationManager:

    def __init__(self, conf: Config):
        self.__debug = int(conf.get(Key.DEBUG_LEVEL))
        self.__reconf_strategy = import_strategy(conf.get(Key.SCALING_STRATEGY), conf)
        if self.__debug > 0:
            print("[DEBUG][RECONF_MNGR] Strategy imported")
            self.__reconf_strategy.print_status(debug=True)

    def get_scaleup_params(self, context: RuntimeContext) -> dict[str, int]:
        return self.__reconf_strategy.scale_up(context)

    def get_scaledown_params(self, context: RuntimeContext) -> dict[str, int]:
        return self.__reconf_strategy.scale_down(context)

    def get_scaling_optimization(self) -> str:
        return self.__reconf_strategy.get_optimization()


def import_strategy(module_path: str, conf: Config) -> BaseReconfigurationStrategy:
    from importlib import import_module

    module_path = module_path.rsplit('.', 1)
    module = import_module(f".{module_path[1]}", module_path[0])

    if int(conf.get(Key.DEBUG_LEVEL)) > 0:
        print("[RECONF_MNGR] Selected strategy is", module_path)

    return module.init_strategy(conf)
