from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy
from transscale.utils.Logger import Logger


class TransprecisionOnly(BaseReconfigurationStrategy):

    def __init__(self, conf: Config, log: Logger):
        super(TransprecisionOnly, self).__init__(conf, log)
        self.strategy_name = "TRANSPRECISION ONLY"
        self.strategy_optimization = DefaultValues.Scaling.Strategy.ScaleUpOptimization.DEFAULT

    def scale_up(self, contex: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_TRANSP}

    def scale_down(self, context: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_TRANSP}


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return TransprecisionOnly(conf, log)

