from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues
from transscale.strategies.ReconfigurationStrategy import BaseReconfigurationStrategy
from transscale.utils.Logger import Logger
import transscale.strategies.ReconfigurationStrategy as rs


class ComboMatrix(BaseReconfigurationStrategy):

    def __init__(self, conf: Config, log: Logger):
        super(ComboMatrix, self).__init__(conf, log)
        self.strategy_name = "COMBO MATRIX"
        self.strategy_optimization.up = DefaultValues.Scaling.Strategy.ScaleOptimization.PREDICTION_MATRIX
        self.strategy_optimization.down = DefaultValues.Scaling.Strategy.ScaleOptimization.PREDICTION_MATRIX

    def scale_up(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        # SCALE UP STRATEGY: as transprecision priority
        #   except if when scaling up parallelism mst is lower than current
        #   scale also tp
        target_par = context.get_current_par()
        target_transp = context.get_current_transp()

        if target_transp == context.get_max_transp():
            # transprecision is at max, get configurations to scale parallelism
            possible_configurations_filtered = [(par, transp) for (par, transp) in possible_configurations
                                                if par > target_par]

            sorted_possibilities = sorted(possible_configurations_filtered,
                                          key=lambda x: (x[0], x[1]))

            if len(sorted_possibilities) > 0:
                # if some configuration is available try to minimize transprecision
                possible_configurations_min_transp = [(par, transp) for (par, transp) in sorted_possibilities
                                                      if transp == 1]

                if len(possible_configurations_min_transp) > 0:
                    # if it exists a configuration with transprecision = 1
                    target_par, target_transp = possible_configurations_min_transp[0]
                else:
                    target_par, target_transp = sorted_possibilities[0]

        else:
            # if transprecision not at max, scale it up
            target_transp = target_transp + 1

        return target_par, target_transp

    def scale_down(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        # SCALE DOWN STRATEGY: always use less resources as possible -> lower parallelism

        sorted_possibilities = sorted(possible_configurations, key=lambda x: (x[0], x[1]))
        self.log.debug(f"[COMBO_STRATEGY] Scale down: Sorted possible conf: {sorted_possibilities}")

        target_par, target_transp = sorted_possibilities[0]
        return target_par, target_transp


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return ComboMatrix(conf, log)
