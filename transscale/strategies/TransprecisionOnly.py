from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy


class TransprecisionOnly(BaseReconfigurationStrategy):

    def __init__(self, conf: Config):
        super(TransprecisionOnly, self).__init__(conf)
        self.strategy_name = "TRANSPRECISION ONLY"
        self.strategy_optimization = DefaultValues.Scaling.Strategy.ScaleUpOptimization.DEFAULT

    def scale_up(self, contex: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_TRANSP}

    def scale_down(self, context: RuntimeContext) -> dict[str, int]:
        return {"method": DefaultValues.Scaling.SCALE_TRANSP}


def init_strategy(conf: Config) -> BaseReconfigurationStrategy:
    return TransprecisionOnly(conf)
