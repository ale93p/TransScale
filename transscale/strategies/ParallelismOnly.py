import transscale.strategies.ReconfigurationStrategy as rs
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy
from transscale.utils.Logger import Logger


class ParallelismOnly(BaseReconfigurationStrategy):

    def __init__(self, conf: Config, log: Logger):
        super(ParallelismOnly, self).__init__(conf, log)
        self.strategy_name = "PARALLELISM ONLY"
        self.strategy_optimization.up = DefaultValues.Scaling.Strategy.ScaleOptimization.SINGLE_CONTROLLER
        self.strategy_optimization.down = DefaultValues.Scaling.Strategy.ScaleOptimization.PREDICTION_MATRIX

    def scale_up(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        return context.get_current_par() + 1, context.get_current_transp()

    def scale_down(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        # get all the configurations where transprecision = 1
        possible_configurations = [(par, transp) for (par, transp) in possible_configurations
                                   if par <= context.get_current_par() and transp == 1]
        # order by parallelism
        possible_configurations = sorted(possible_configurations, key=lambda x: (x[0], x[1]))

        # return the configuration with the lowest parallelism
        if len(possible_configurations) > 0:
            return possible_configurations[0]
        else:
            return context.get_current_state()


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return ParallelismOnly(conf, log)
