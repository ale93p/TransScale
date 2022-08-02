from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.utils.Logger import Logger


class ParallelismPriority(BaseReconfigurationStrategy):

    def __init__(self, conf: Config, log: Logger):
        super(ParallelismPriority, self).__init__(conf, log)
        self.strategy_name = "PARALLELISM PRIORITY"
        self.strategy_optimization = DefaultValues.Scaling.Strategy.ScaleUpOptimization.DEFAULT

    def scale_up(self, context: RuntimeContext) -> dict[str, int]:
        if context.get_current_par() >= self.max_par:
            return {"method": DefaultValues.Scaling.SCALE_TRANSP}
        else:
            return {"method": DefaultValues.Scaling.SCALE_PAR}

    def scale_down(self, context: RuntimeContext) -> dict[str, int]:
        if context.get_current_transp() > 1:
            return {"method": DefaultValues.Scaling.SCALE_TRANSP}
        else:
            return {"method": DefaultValues.Scaling.SCALE_PAR}


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return ParallelismPriority(conf, log)
