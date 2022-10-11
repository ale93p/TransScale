from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key
from transscale.utils.Logger import Logger

RSP_METHOD = "method"
RSP_PAR = "new_par"
RSP_TRANSP = "new_transp"


class StrategyOptimization:
    up = None
    down = None


class BaseReconfigurationStrategy:

    def __init__(self, conf: Config, log: Logger):
        self.max_par = int(conf.get(Key.MAX_PAR))
        self.max_transp = int(conf.get(Key.MAX_TRANSP))
        self.strategy_optimization = StrategyOptimization()
        self.strategy_name = "BaseReconfigurationStrategy"

        self.log = log

    def scale_up(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        pass

    def scale_down(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        pass

    def get_optimization(self) -> StrategyOptimization:
        return self.strategy_optimization

    def print_status(self) -> None:
        self.log.debug("", end="")
        self.log.info(f"[RECONF_STRAT] Using {self.strategy_name} strategy")
        self.log.debug("", end="")
        self.log.info(f"[RECONF_STRAT] ScaleUp Optimization is DEFAULT ({self.strategy_optimization})")


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return BaseReconfigurationStrategy(conf, log)
