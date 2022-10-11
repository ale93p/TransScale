from transscale.components.MeasurementsManager import MeasurementsManager
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key
from transscale.utils.DefaultValues import DefaultValues

from transscale.utils.Logger import Logger
from transscale.utils.prediction.models.ParallelismModel import ParallelismModel
from transscale.utils.prediction.models.TransprecisionModel import TransprecisionModel

from numpy import ndarray, zeros


class PredictionMatrix:

    def __init__(self, conf: Config, log: Logger):
        self.__log = log

        self.__max_par = conf.get_int(Key.MAX_PAR)
        self.__max_transp = conf.get_int(Key.MAX_TRANSP)

        self.__par_model = ParallelismModel(log)
        self.__transp_model = TransprecisionModel(log)

        self.__prediction_matrix = zeros([self.__max_par + 1, self.__max_transp + 1])

    def __build_queue(self, measurements: ndarray, prediction_matrix: ndarray) -> list:
        queue = []

        for p in range(1, self.__max_par + 1):
            n_real = len([x for x in measurements[p] if x > 0])
            n_pred = len([x for x in prediction_matrix[p] if x > 0])
            if n_pred != 0 and n_pred != self.__max_transp:
                queue.append(("PAR", p, n_real, n_pred))

        for t in range(1, self.__max_transp + 1):
            n_real = len([x for x in measurements.transpose()[t] if x > 0])
            n_pred = len([x for x in prediction_matrix.transpose()[t] if x > 0])
            if n_pred != 0 and n_pred != self.__max_par:
                queue.append(("TRANSP", t, n_real, n_pred))

        queue = sorted(queue, key=lambda x: (x[2], x[3]), reverse=True)

        return queue

    def __train_transprecision_model(self, num_measurements: int, measurements_array: list) -> None:
        if num_measurements > 1:
            self.__transp_model.train_full_model(measurements_array)
        else:
            self.__transp_model.train_min_model(measurements_array)

    def __train_parallelism_model(self, num_measurements: int, measurements_array: list, network_array: list) -> None:
        if num_measurements > 2:
            self.__par_model.train_full_model(measurements_array, network_array)
        elif num_measurements > 1:
            self.__par_model.train_reduced_model(measurements_array, network_array)
        else:
            self.__par_model.train_min_model(measurements_array)

    def __predict(self, elem: list, network_array: list) -> None:

        param, val, real, pred = elem
        if param == "TRANSP":
            # it means that the row of transp = val is the one with 'pred' elements
            # so it means fixing the measurements to transp = val and predict it for the missing values of parallelism
            # hence must use parallelism model
            measurements_array = list(self.__prediction_matrix.transpose()[val])

            self.__train_parallelism_model(pred, measurements_array, network_array)

            for i in range(1, self.__max_par + 1):
                if self.__prediction_matrix[i, val] <= 0:
                    self.__prediction_matrix[i, val] = int(self.__par_model.get_mst(i, network_array[i]))

        elif param == "PAR":
            self.__train_transprecision_model(pred, list(self.__prediction_matrix[val]))

            for i in range(1, self.__max_transp + 1):
                if self.__prediction_matrix[val, i] <= 0:
                    self.__prediction_matrix[val, i] = int(self.__transp_model.get_mst(i))

    def __update(self, measurements: MeasurementsManager, network_array: list) -> None:

        measurements_array = measurements.get_measurements()
        self.__prediction_matrix = measurements_array.copy()

        predict_queue = self.__build_queue(measurements_array, self.__prediction_matrix)

        while len(predict_queue) > 0:
            predict_elem = predict_queue[0]
            self.__predict(predict_elem, network_array)
            predict_queue = self.__build_queue(measurements_array, self.__prediction_matrix)

        self.__log.debugg(f"[PRED_MATRIX] Updated prediction matrix is:\n {self.__prediction_matrix}")

    def update_matrix(self, measurements: MeasurementsManager):
        network_array = measurements.get_network_distance()
        self.__update(measurements, network_array)

    def get_scaling_possibilities(self, context: RuntimeContext, target_tput: int,
                                  low_throughput_threshold: int = DefaultValues.Scaling.Combined.threshold
                                  ) -> list:

        self.__log.debug(f"[PRED_MATRIX] Current prediction matrix is:\n {self.__prediction_matrix}")

        current_par, current_transp = context.get_current_state()

        pos = []
        new_transp = 1
        for new_par in range(1, self.__max_par + 1):
            for new_transp in range(1, self.__max_transp + 1):
                new_mst = self.__prediction_matrix[new_par][new_transp]

                throughput_diff = new_mst - target_tput
                throughput_diff_perc = 0 if throughput_diff == 0 else 100 * throughput_diff / new_mst

                debug_status = f"[PRED_MATRIX] "\
                               f"\tpar, transp: {new_par}, {new_transp} "\
                               f"target tput: {target_tput} "\
                               f"new_tput %: {new_mst} "\
                               f"tput diff %: {throughput_diff_perc} "

                if throughput_diff_perc > low_throughput_threshold \
                        and (new_par, new_transp) != (current_par, current_transp):
                    pos.append((new_par, new_transp))
                    self.__log.debug(debug_status + "ADDED")
                    break
                else:
                    self.__log.debug(debug_status)

            if new_transp == 1:
                break

        self.__log.debug(f"[PRED_MATRIX] Scaling Possibilities: {pos}")
        return pos
