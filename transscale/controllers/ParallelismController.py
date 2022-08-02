from .models.ParallelismModel import ParallelismModel
from transscale.components.MeasurementsManager import MeasurementsManager
from transscale.components.ResourceManager import ResourceManager
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues, ConfigKeys as Key
from ..utils.Logger import Logger


class ParallelismController:

    def __init__(self, conf: Config, log: Logger):
        self.__debug = int(conf.get(Key.DEBUG_LEVEL))
        self.__log = log
        self.__perf_model = ParallelismModel(log)
        pass

    def scaleup(self, context: RuntimeContext, measurements: MeasurementsManager,
                res_manager: ResourceManager, optimization: str) -> int:

        self.__log.info("\n[PAR_CTRL] Reconf Parallelism: Scale Up")

        par = context.get_current_par()

        target_par = par

        network_distance = measurements.get_network_distance()

        if optimization == DefaultValues.Scaling.Strategy.ScaleUpOptimization.DEFAULT:

            self.__log.info("[PAR_CTRL] Scale up strategy: Default")

            if par == context.get_max_par():
                self.__log.info("[PAR_CTRL] Number of replicas already at maximum")

            else:
                num_measurements = measurements.get_measurements_num_par(context.get_current_transp())
                measurement_array = measurements.get_measurements(transp=context.get_current_transp())

                self.__log.debug(f"[PAR_CTRL] Num measurements: {num_measurements}")
                self.__log.debug(f"[PAR_CTRL] Measurement array: {measurement_array}")

                if num_measurements == 1:
                    self.__perf_model.train_min_model(measurement_array)

                elif num_measurements == 2:
                    self.__perf_model.train_reduced_model(measurement_array, network_distance)

                elif num_measurements >= 3:
                    self.__perf_model.train_full_model(measurement_array, network_distance)

                target_par = par + 1

        elif optimization == DefaultValues.Scaling.Strategy.ScaleUpOptimization.CUSTOM_OPTIMIZATION:
            from math import ceil

            self.__log.info("[PAR_CTRL] Scale up strategy: Custom Optimization")
            num_measurements = measurements.get_measurements_num_par(1)
            measurement_array = measurements.get_measurements(transp=1)
            self.__log.debug(f"[PAR_CTRL] Num measurements: {num_measurements}")
            self.__log.debug(f"[PAR_CTRL] Measurement array: {measurement_array}")

            current_source_rate = context.get_source_input_rate()

            if num_measurements == 1:
                self.__perf_model.train_min_model(measurement_array)

                target_par = ceil(current_source_rate / self.__perf_model.get_status()["alpha"])

            elif num_measurements >= 2:
                if num_measurements == 2:
                    self.__perf_model.train_reduced_model(measurement_array, network_distance)
                else:
                    self.__perf_model.train_full_model(measurement_array, network_distance)

                target_par = par
                target_nd_max = network_distance[target_par]
                target_mst = self.__perf_model.get_mst(target_par, target_nd_max)

                while (target_par < context.get_max_par()) and (target_mst < current_source_rate):
                    target_par += 1
                    target_nd_max = network_distance[target_par]
                    target_mst = self.__perf_model.get_mst(target_par, target_nd_max)

            # we found the target parallelism to sustain the current MST resetting transprecision
            # now we "scale up" the parallelism
            target_par += 1

            if target_par > context.get_max_par():
                self.__log.info(f"[PAR_CTRL] Number of replicas cannot be more than {context.get_max_par()}")
                target_par = context.get_max_par()

            if self.__perf_model.get_mst(target_par) <= current_source_rate:
                self.__log.info(f"[PAR_CTRL] WARNING: next MST may be lower than current one")

        else:
            self.__log.info("[PAR_CTRL] ERROR: Wrong or no optimization chosen")

        self.__perf_model.print_model_status()

        next_nd_max = res_manager.get_max_network_delay(target_par)
        self.__log.info(f"\ttarget par: {target_par}")
        self.__log.info(f"\tnext max nd: {next_nd_max}")
        self.__log.info(f"\ttarget mst: {self.__perf_model.get_mst(target_par, network_distance[target_par])}")

        if target_par != par:
            return target_par

        return par

    def scaledown(self, context: RuntimeContext, measurements: MeasurementsManager,
                  low_throughput_threshold: int = DefaultValues.Scaling.Parallelism.threshold) -> int:
        from math import ceil

        self.__log.info("\n[PAR_CTRL] Reconf Parallelism: Scale Down")

        par = context.get_current_par()
        current_source_rate = context.get_source_input_rate()

        ignored = False
        num_measurements = measurements.get_measurements_num_par(context.get_current_transp())
        measurement_array = measurements.get_measurements(transp=context.get_current_transp())
        network_distance = measurements.get_network_distance()

        target_par = par
        current_mst = self.__perf_model.get_mst(par, network_distance[par])
        throughput_diff = current_mst - current_source_rate
        throughput_diff_perc = 100 * throughput_diff / current_mst

        if num_measurements == 1:

            self.__perf_model.train_min_model(measurement_array)

            if (throughput_diff_perc > low_throughput_threshold) and (par > 1):
                self.__log.warning(f"[PAR_CTRL] Current throughput of the operator is {throughput_diff_perc}"
                                   f" percent less than maximum throughput.")

                alpha = self.__perf_model.get_status()["alpha"]
                target_par = ceil(current_source_rate / alpha)

                ignored = (par == target_par)

                if not ignored:
                    self.__log.info(f"\ttarget par: {target_par}")
                    self.__log.info(f"\ttarget mst: {self.__perf_model.get_mst(target_par)}")

        elif num_measurements >= 2:
            if num_measurements == 2:
                self.__perf_model.train_reduced_model(measurement_array, network_distance)
            else:
                self.__perf_model.train_full_model(measurement_array, network_distance)

            target_mst = current_mst
            old_par = target_par

            while (throughput_diff_perc > low_throughput_threshold) \
                    and (target_par >= 2) and (target_mst > current_source_rate):
                old_par = target_par

                target_par -= 1

                # Automatically use the parameters computed with 2 or 3 or more measurements
                # differently from the bash version we don't need to re-run the fitting function here
                # we already have the values
                target_mst = self.__perf_model.get_mst(target_par, network_distance[target_par])
                throughput_diff = target_mst - current_source_rate
                throughput_diff_perc = 100 * throughput_diff / target_mst

            if throughput_diff_perc > 0:
                target_par = old_par

            ignored = (par == target_par)
            if not ignored:
                self.__log.info(f"\ttarget par: {target_par}")
                self.__log.info(f"\ttarget mst: {self.__perf_model.get_mst(target_par, network_distance[target_par])}")

        if target_par != par:
            return target_par

        elif ignored:
            self.__log.info("[PAR_CTRL] Reconfiguration ignored, as the new parallelism is the same.")

        return par
