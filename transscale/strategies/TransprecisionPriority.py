import transscale.strategies.ReconfigurationStrategy as rs
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues


class TransprecisionPriority(rs.BaseReconfigurationStrategy):

    def __init__(self, conf: Config):
        super(TransprecisionPriority, self).__init__(conf)
        self.strategy_name = "TRANSPRECISION PRIORITY"
        self.strategy_optimization = DefaultValues.Scaling.Strategy.ScaleUpOptimization.CUSTOM_OPTIMIZATION

    def scale_up(self, context: RuntimeContext) -> dict[str, int]:
        if context.get_current_transp() >= self.max_transp:
            return {
                rs.RSP_METHOD: DefaultValues.Scaling.SCALE_PAR,
                rs.RSP_TRANSP: 1
            }
        else:
            return {rs.RSP_METHOD: DefaultValues.Scaling.SCALE_TRANSP}

    def scale_down(self, context: RuntimeContext) -> dict[str, int]:
        if context.get_current_transp() < 2:
            return {rs.RSP_METHOD: DefaultValues.Scaling.SCALE_PAR}
        else:
            return {rs.RSP_METHOD: DefaultValues.Scaling.SCALE_TRANSP}


def init_strategy(conf: Config) -> rs.BaseReconfigurationStrategy:
    return TransprecisionPriority(conf)
