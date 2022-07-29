from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy


class ParallelismOnly(BaseReconfigurationStrategy):

    def __init__(self, conf: Config):
        super().__init__(conf)
        self.strategy_name = "PARALLELISM ONLY"
        self.strategy_optimization = DefaultValues.Scaling.Strategy.ScaleUpOptimization.DEFAULT

    def scale_up(self, contex: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_PAR}

    def scale_down(self, context: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_PAR}


def init_strategy(conf: Config) -> BaseReconfigurationStrategy:
    return ParallelismOnly(conf)

