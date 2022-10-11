from transscale.utils.prediction.models.TransprecisionModel import TransprecisionModel
from transscale.components.MeasurementsManager import MeasurementsManager
from transscale.components.RuntimeContext import RuntimeContext
from ..utils.Config import Config
from ..utils.DefaultValues import DefaultValues, ConfigKeys as Key
from ..utils.Logger import Logger


def convert_throughput(context: RuntimeContext, convert_method: str, throughput: int) -> int:
    if convert_method == DefaultValues.Scaling.Transprecision.SAMPLING_DIRECT:
        return throughput * context.get_current_transp()
    else:
        return throughput


class TransprecisionController:

    def __init__(self, conf: Config, log: Logger):
        self.__debug = int(conf.get(Key.DEBUG_LEVEL))
        self.__log = log
        self.__perf_model = TransprecisionModel(log)
        pass

    def scaleup(self, context: RuntimeContext, measurements: MeasurementsManager) -> int:
        self.__log.info("\n[TRANSP_CTRL] Reconf Transprecision: Scale Up")

        transp = context.get_current_transp()
        target_transp = transp

        if transp == context.get_max_transp():
            self.__log.info("[TRANSP_CTRL]: Transprecision level already at maximum")

        else:
            num_measurements = measurements.get_measurements_num_transp(context.get_current_par())
            measurement_array = measurements.get_measurements(par=context.get_current_par())

            self.__log.debug(f"[TRANSP_CTRL] Num measurements: {num_measurements}")
            self.__log.debug(f"[TRANSP_CTRL] Measurement array: {measurement_array}")

            if num_measurements == 1:
                self.__perf_model.train_min_model(measurement_array)

            elif num_measurements >= 2:
                self.__perf_model.train_full_model(measurement_array)

            target_transp = transp + 1

            self.__perf_model.print_model_status()

            self.__log.info(f"\n[TRANSP_CTRL]target transp: {target_transp}")
            self.__log.info(f"\n[TRANSP_CTRL]target mst: {self.__perf_model.get_mst(target_transp)}")

        if target_transp != transp:
            return target_transp

        return transp

    def scaledown(self, context: RuntimeContext, measurements: MeasurementsManager,
                  low_throughput_threshold: int = DefaultValues.Scaling.Transprecision.threshold) -> int:
        from math import ceil

        self.__log.info("\n[TRANSP_CTRL] Reconf Transprecision: Scale Down")

        transp = context.get_current_transp()
        operator_throughput = convert_throughput(context, DefaultValues.Scaling.Transprecision.SAMPLING_DIRECT,
                                                 context.get_operator_throughput())

        ignored = False
        num_measurements = measurements.get_measurements_num_transp(context.get_current_par())
        measurement_array = measurements.get_measurements(par=context.get_current_par())

        target_transp = transp
        current_mst = self.__perf_model.get_mst(transp)
        throughput_diff = current_mst - operator_throughput
        throughput_diff_perc = 100 * throughput_diff / current_mst

        if num_measurements == 1:
            self.__perf_model.train_min_model(measurement_array)

            if (throughput_diff_perc > low_throughput_threshold) and (transp > 1):
                self.__log.info(f"[TRANSP_CTRL] WARNING: Current throughput of the operator is {throughput_diff_perc}"
                               f" percent less than maximum throughput.")

                alpha = self.__perf_model.get_status()["alpha"]
                target_transp = ceil(operator_throughput / alpha)

                ignored = (transp == target_transp)

                if not ignored:
                    self.__log.info(f"\n[TRANSP_CTRL]target transp: {target_transp}")
                    self.__log.info(f"\n[TRANSP_CTRL]target mst: {self.__perf_model.get_mst(target_transp)}")

        elif num_measurements >= 2:
            self.__perf_model.train_full_model(measurement_array)

            target_mst = current_mst
            old_transp = target_transp

            while (throughput_diff_perc > low_throughput_threshold) \
                    and (target_transp >= 2) and (target_mst > operator_throughput):
                old_transp = target_transp
                target_transp -= 1

                target_mst = self.__perf_model.get_mst(target_transp)
                throughput_diff = target_mst - operator_throughput
                throughput_diff_perc = 100 * throughput_diff / target_mst

            if throughput_diff_perc > 0:
                target_transp = old_transp

            ignored = (transp == target_transp)
            if not ignored:
                self.__log.info(f"\n[TRANSP_CTRL]target trasnp: {target_transp}")
                self.__log.info(f"\n[TRANSP_CTRL]target mst: {self.__perf_model.get_mst(target_transp)}")

        if target_transp != transp:
            return target_transp

        elif ignored:
            self.__log.info("[PAR_CTRL] Reconfiguration ignored, as the new parallelism is the same.")

        return transp
