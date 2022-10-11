from transscale.components.MeasurementsManager import MeasurementsManager
from transscale.components.ReconfigurationManager import ReconfigurationManager
from transscale.components.RuntimeContext import RuntimeContext
from transscale.controllers.TransprecisionController import convert_throughput
from transscale.utils.prediction.PredictionMatrix import PredictionMatrix
from transscale.utils.prediction.models.ParallelismModel import ParallelismModel
from transscale.utils.prediction.models.TransprecisionModel import TransprecisionModel
from transscale.utils.Config import Config
from transscale.utils.Logger import Logger
from transscale.utils.DefaultValues import DefaultValues, ConfigKeys as Key


class CombinedController:

    def __init__(self, conf: Config, log: Logger):
        self.__debug = int(conf.get(Key.DEBUG_LEVEL))
        self.__log = log

        self.__reconf_manager = ReconfigurationManager(conf, log)
        self.__prediction_matrix = PredictionMatrix(conf, log)

    def scaleup(self, context: RuntimeContext, measurements: MeasurementsManager):
        self.__log.info("\n[COMBO_CTRL] Reconf: Scale Up")

        operator_throughput = convert_throughput(context, DefaultValues.Scaling.Transprecision.SAMPLING_DIRECT,
                                                 context.get_operator_throughput())

        self.__prediction_matrix.update_matrix(measurements)
        possible_configurations = self.__prediction_matrix \
            .get_scaling_possibilities(context, operator_throughput)

        target_par, target_transp = self.__reconf_manager.get_scaleup_target(possible_configurations, context)

        if target_par > context.get_max_par() or target_transp > context.get_max_transp():
            self.__log.warning(f"[COMBO_CTRL] Target configuration goes above maximum levels "
                               f"(par={target_par}, transp={target_transp}\n"
                               f"\tReverting to current status")
            target_par = context.get_current_par()
            target_transp = context.get_current_transp()

        return target_par, target_transp

    def scaledown(self, context: RuntimeContext) -> tuple[int, int]:
        self.__log.info("\n[COMBO_CTRL] Reconf: Scale Down")

        operator_throughput = convert_throughput(context, DefaultValues.Scaling.Transprecision.SAMPLING_DIRECT,
                                                 context.get_operator_throughput())

        possible_configurations = self.__prediction_matrix \
            .get_scaling_possibilities(context, operator_throughput)

        target_par, target_transp = self.__reconf_manager.get_scaledown_target(possible_configurations, context)

        if target_par < 1 or target_transp < 1:
            self.__log.warning(f"[COMBO_CTRL] Target configuration goes below minimum levels "
                               f"(par={target_par}, transp={target_transp}\n"
                               f"\tReverting to current status")
            target_par = context.get_current_par()
            target_transp = context.get_current_transp()

        return target_par, target_transp
