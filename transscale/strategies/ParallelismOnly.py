from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy
from transscale.utils.Logger import Logger


class ParallelismOnly(BaseReconfigurationStrategy):

    def __init__(self, conf: Config, log: Logger):
        super(ParallelismOnly, self).__init__(conf, log)
        self.strategy_name = "PARALLELISM ONLY"
        self.strategy_optimization = DefaultValues.Scaling.Strategy.ScaleUpOptimization.DEFAULT

    def scale_up(self, contex: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_PAR}

    def scale_down(self, context: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_PAR}


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return ParallelismOnly(conf, log)

