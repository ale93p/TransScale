from .PerformanceModel import BasePerformanceModel

from scipy.optimize import curve_fit


class ParallelismModel(BasePerformanceModel):

    def __init__(self):
        super(ParallelismModel, self).__init__()
        self.__alpha: float = 1.0
        self.__beta: float = 1.0
        self.__gamma: float = 0.0

    def get_status(self) -> dict[str, float]:
        return {"alpha": self.__alpha, "beta": self.__beta, "gamma": self.__gamma}

    def print_model_status(self) -> None:
        print("[PAR_MDL] Model Status")
        print("\talpha:", self.__alpha)
        print("\tbeta:", self.__beta)
        print("\tgamma:", self.__gamma)

    def curve_fit_min(self, measurements_array: list[int]) -> None:
        par = [i for i in range(0, len(measurements_array)) if measurements_array[i] > 0][0]
        mst = measurements_array[par]

        self.__alpha = mst / par

    def curve_fit_reduced(self, measurements_array: list[int], network_array: list[int]) -> None:
        par_data = [i for i in range(0, len(measurements_array)) if measurements_array[i] > 0]
        net_data = [network_array[i] for i in par_data]
        mst_data = [measurements_array[i] for i in par_data]

        popt, _ = curve_fit(lambda x, alpha, gamma: alpha * x[0] - gamma * x[1],
                            (par_data, net_data), mst_data)

        self.__alpha, self.__gamma = popt

    def curve_fit_full(self, measurements_array: list[int], network_array: list[int]) -> None:
        par_data = [i for i in range(0, len(measurements_array)) if measurements_array[i] > 0]
        net_data = [network_array[i] for i in par_data]
        mst_data = [measurements_array[i] for i in par_data]

        popt, _ = curve_fit(lambda x, alpha, beta, gamma: alpha * x[0] ** beta - gamma * x[1],
                            (par_data, net_data), mst_data)

        self.__alpha, self.__beta, self.__gamma = popt

    def get_mst(self, par: int, nd: int = 0) -> float:
        return self.__alpha * par ** self.__beta - self.__gamma**nd
