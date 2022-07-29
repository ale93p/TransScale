from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key

RSP_METHOD = "method"
RSP_PAR = "new_par"
RSP_TRANSP = "new_transp"


class BaseReconfigurationStrategy:

    def __init__(self, conf: Config):
        self.max_par = int(conf.get(Key.MAX_PAR))
        self.max_transp = int(conf.get(Key.MAX_TRANSP))
        self.strategy_optimization = None
        self.strategy_name = "BaseReconfigurationStrategy"

    def scale_up(self, context: RuntimeContext) -> dict[str, int]:
        pass

    def scale_down(self, context: RuntimeContext) -> dict[str, int]:
        pass

    def get_optimization(self) -> str:
        return self.strategy_optimization

    def print_status(self, debug:bool = False) -> None:
        if debug: print("[DEBUG]", end="")
        print(f"[RECONF_STRAT] Using {self.strategy_name} strategy")
        if debug: print("[DEBUG]", end="")
        print(f"[RECONF_STRAT] ScaleUp Optimization is DEFAULT ({self.strategy_optimization})")


def init_strategy(conf: Config) -> BaseReconfigurationStrategy:
    return BaseReconfigurationStrategy(conf)
