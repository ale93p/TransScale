import transscale.strategies.ReconfigurationStrategy as rs
from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.utils.Logger import Logger


class ParallelismPriority(BaseReconfigurationStrategy):

    def __init__(self, conf: Config, log: Logger):
        super(ParallelismPriority, self).__init__(conf, log)
        self.strategy_name = "PARALLELISM PRIORITY"
        self.strategy_optimization.up = DefaultValues.Scaling.Strategy.ScaleOptimization.SINGLE_CONTROLLER
        self.strategy_optimization.down = DefaultValues.Scaling.Strategy.ScaleOptimization.PREDICTION_MATRIX

    def scale_up(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        target_par = context.get_current_par() + 1
        target_transp = context.get_current_transp()

        if target_par > context.get_max_par():
            target_par = context.get_max_par()
            target_transp += 1

        return target_par, target_transp

    def scale_down(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        target_par = context.get_current_par()
        target_transp = context.get_current_transp()

        if target_transp > 1:
            # get all the configurations where parallelism = current
            possible_configurations = [(par, transp) for (par, transp) in possible_configurations
                                       if par == target_par and transp <= target_transp]
            # order by parallelism
            possible_configurations = sorted(possible_configurations, key=lambda x: (x[0], x[1]))
            # return the configuration with lowest transprecision
            if len(possible_configurations) > 0:
                return possible_configurations[0]

        else:
            # get all the configurations where parallelism = 1
            possible_configurations = [(par, transp) for (par, transp) in possible_configurations
                                       if par <= target_par and transp == 1]
            # order by parallelism
            possible_configurations = sorted(possible_configurations, key=lambda x: (x[0], x[1]))
            # return the configuration with lowest transprecision
            if len(possible_configurations) > 0:
                return possible_configurations[0]

        return context.get_current_state()


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return ParallelismPriority(conf, log)
