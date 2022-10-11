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
        # SCALE UP STRATEGY: always use less resources as possible -> lower parallelism
        target_par = context.get_current_par()

        sorted_possibilities = sorted([conf for conf in possible_configurations if conf[0] >= target_par],
                                      key=lambda x: (x[0], x[1]))
        self.log.debug(f"[COMBO_STRATEGY] Scale up: Sorted possible conf: {sorted_possibilities}")

        target_par, target_transp = sorted_possibilities[0]
        return target_par, target_transp

    def scale_down(self, possible_configurations: list, context: RuntimeContext) -> tuple[int, int]:
        # SCALE DOWN STRATEGY: always use less resources as possible -> lower parallelism

        sorted_possibilities = sorted(possible_configurations, key=lambda x: (x[0], x[1]))
        self.log.debug(f"[COMBO_STRATEGY] Scale down: Sorted possible conf: {sorted_possibilities}")

        target_par, target_transp = sorted_possibilities[0]
        return target_par, target_transp


def init_strategy(conf: Config, log: Logger) -> BaseReconfigurationStrategy:
    return ComboMatrix(conf, log)
