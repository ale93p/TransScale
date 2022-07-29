from .PerformanceModel import BasePerformanceModel

from scipy.optimize import curve_fit


class TransprecisionModel(BasePerformanceModel):

    def __init__(self):
        super(TransprecisionModel, self).__init__()
        self.__alpha: float = 1.0
        self.__beta: float = 1.0

    def get_status(self) -> dict[str, float]:
        return {"alpha": self.__alpha, "beta": self.__beta}

    def print_model_status(self) -> None:
        print("[TRANS_MDL] Model Status")
        print("\talpha:", self.__alpha)
        print("\tbeta:", self.__beta)

    def curve_fit_min(self, measurements_array: list[int]) -> None:
        transp = [i for i in range(0, len(measurements_array)) if measurements_array[i] > 0][0]
        mst = measurements_array[transp]

        self.__alpha = mst / transp

    def curve_fit_full(self, measurements_array: list[int], network_array: list[int] = None) -> None:
        transp_data = [i for i in range(0, len(measurements_array)) if measurements_array[i] > 0]
        mst_data = [measurements_array[i] for i in transp_data]

        popt, _ = curve_fit(lambda x, alpha, beta: alpha * x ** beta, transp_data, mst_data)

        self.__alpha, self.__beta = popt

    def get_mst(self, transp: int) -> float:
        return self.__alpha * transp ** self.__beta
