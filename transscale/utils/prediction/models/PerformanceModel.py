class BasePerformanceModel:

    def __init__(self):
        pass

    def curve_fit_min(self, measurements_array: list[int]) -> None:
        pass

    def train_min_model(self, measurements_array: list[int]) -> None:
        self.curve_fit_min(measurements_array)

    def curve_fit_reduced(self, measurements_array: list[int], network_array: list[int]) -> None:
        pass

    def train_reduced_model(self, measurements_array: list[int], network_array: list[int]) -> None:
        self.curve_fit_reduced(measurements_array, network_array)

    def curve_fit_full(self, measurements_array: list[int], network_array: list[int]) -> None:
        pass

    def train_full_model(self, measurements_array: list[int], network_array: list[int] = None) -> None:
        self.curve_fit_full(measurements_array, network_array)

    def print_model_status(self) -> None:
        pass

    def get_status(self) -> dict[str, float]:
        pass

