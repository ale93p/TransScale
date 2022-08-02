from typing import Tuple
import requests as req
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key
from transscale.utils.Logger import Logger


class RuntimeState:

    def __init__(self):
        self.parallelism = 1
        self.transprecision = 0

    def __eq__(self, other: any) -> bool:
        if isinstance(other, RuntimeState):
            return (self.parallelism == other.parallelism) and (self.transprecision == other.transprecision)
        return False

    def __ne__(self, other: any) -> bool:
        return not self.__eq__(other)


class RuntimeContext:

    def __init__(self, conf: Config, log: Logger):
        self.__job_id = None
        self.__operator_id = None
        self.__operator_name = None
        self.__source_id = None

        self.__job_url = None
        self.__operator_url = None
        self.__transprecision_url = None
        self.__source_url = None
        self.__source_backpressure_url = None
        self.__source_tput_url = None
        self.__operator_tput_url = None

        self.__job_runtime = 0
        self.__backpressure = 0
        self.__source_throughput = 0
        self.__operator_throughput = 0

        self.__current_state = RuntimeState()
        self.__target_state = RuntimeState()

        self.__max_par = int(conf.get(Key.MAX_PAR))
        self.__max_transp = int(conf.get(Key.MAX_TRANSP))

        self.__cluster_ip = f"{conf.get(Key.FLINK_HOST)}:{conf.get(Key.FLINK_PORT)}"
        self.__cluster_url = f"http://{self.__cluster_ip}"
        self.__jobs_url = f"{self.__cluster_url}/jobs"

        self.__debug = int(conf.get(Key.DEBUG_LEVEL))
        self.__log = log

    def __get_current_job_runtime(self):
        return req.get(self.__job_url).json()["duration"]

    def __get_current_backpressure(self) -> int:
        self.__log.debug(f"\tGetting source backpressure from url {self.__source_backpressure_url}")
        bp = int(req.get(self.__source_backpressure_url).json()["subtasks"][0]["ratio"])
        return 0 if bp is None else bp

    def __get_current_source_throughput(self) -> int:
        self.__log.debug(f"\tGetting source throughput from url {self.__source_tput_url}")
        return int(float(req.get(self.__source_tput_url).json()[0]["value"]))

    def __get_current_operator_throughput(self) -> int:
        self.__log.debug(f"\tGetting operator throughput from url {self.__operator_tput_url}")
        return int(float(req.get(self.__operator_tput_url).json()[0]["sum"]))

    def update_job_details(self) -> None:
        self.__log.info(f"[MONITOR]: Retrieving job details from {self.__cluster_ip} ...")

        self.__job_id = req.get(self.__jobs_url).json()["jobs"][0]["id"]
        self.__job_url = f"{self.__jobs_url}/{self.__job_id}"

        operator_res = req.get(self.__job_url).json()["vertices"][1]
        self.__operator_id = operator_res["id"]
        self.__operator_name = operator_res["name"]

        self.__operator_url = f"{self.__job_url}/vertices/{self.__operator_id}"
        self.__transprecision_url = f"{self.__operator_url}/metrics?get=0.{self.__operator_name}.TransprecisionLevel"

        self.__source_id = req.get(self.__job_url).json()["vertices"][0]["id"]
        self.__source_url = f"{self.__job_url}/vertices/{self.__source_id}"

        self.__source_backpressure_url = f"{self.__source_url}/backpressure"
        self.__source_tput_url = f"{self.__source_url}/metrics?get=0.numRecordsOutPerSecond"

        # TODO: must be multiplied by transp level, or consider InPerSecond?
        self.__operator_tput_url = \
            f"{self.__operator_url}/subtasks/metrics?get={self.__operator_name}.numRecordsOutPerSecond"

    def update_state(self) -> None:
        try:
            self.__log.debug(f"\tGetting operator parallelism from url {self.__job_url}")
            operator_res = req.get(self.__job_url).json()["vertices"][1]
            self.__current_state.parallelism = int(operator_res["parallelism"])
        except IndexError:
            self.__log.info(f"[MONITOR] ERROR: Unable to retrieve parallelism, setting it to 0")
            self.__current_state.parallelism = 0

        try:
            self.__log.debug(f"\tGetting operator transprecision from url {self.__transprecision_url}")
            transp_res = req.get(self.__transprecision_url).json()[0]
            self.__current_state.transprecision = int(transp_res["value"])
        except IndexError:
            self.__log.error(f"[MONITOR] Unable to retrieve transprecision, setting it to 0")
            self.__current_state.transprecision = 0

    def print_details(self) -> None:
        self.__log.info(f"[MONITOR]:: Details about the job:")
        self.__log.info(f"\t Job ID: {self.__job_id}")
        self.__log.info(f"\t Target Operator ID: {self.__operator_id}")
        self.__log.info(f"\t Target Operator Name: {self.__operator_name}")
        self.__log.info(f"\t Target Operator Parallelism: {self.__current_state.parallelism}")
        self.__log.info(f"\t Target Operator Transprecision: {self.__current_state.transprecision}")

    def update_job_runtime_metrics(self) -> None:
        self.__job_runtime = self.__get_current_job_runtime()
        self.__backpressure = self.__get_current_backpressure()
        self.__source_throughput = self.__get_current_source_throughput()
        self.__operator_throughput = self.__get_current_operator_throughput()

    def set_target_state(self, par: int, transp: int) -> None:
        self.__target_state.parallelism = par
        self.__target_state.transprecision = transp

    def print_job_runtime_metrics(self) -> None:
        self.__log.info(f"[MONITOR]:: Runtime performance metrics:")
        self.__log.info(f"\t Job Run Time: {self.__job_runtime} ms")
        self.__log.info(f"\t Source Backpressure Ratio: {self.__backpressure}")
        self.__log.info(f"\t Source Input Ratio: {self.__source_throughput}")

    def get_current_state(self) -> Tuple[int, int]:
        return self.__current_state.parallelism, self.__current_state.transprecision

    def get_target_state(self) -> Tuple[int, int]:
        return self.__target_state.parallelism, self.__target_state.transprecision

    def get_backpressure(self) -> int:
        return self.__backpressure

    def get_source_input_rate(self) -> int:
        return self.__source_throughput

    def get_operator_throughput(self) -> int:
        return self.__operator_throughput

    def get_current_par(self) -> int:
        return self.get_current_state()[0]

    def get_current_transp(self) -> int:
        return self.get_current_state()[1]

    def get_target_par(self) -> int:
        return self.get_target_state()[0]

    def get_target_transp(self) -> int:
        return self.get_target_state()[1]

    def get_max_par(self) -> int:
        return self.__max_par

    def get_max_transp(self) -> int:
        return self.__max_transp

    def get_cluster_ip(self) -> str:
        return self.__cluster_ip

    def get_job_id(self) -> str:
        return self.__job_id

    def is_reconf_required(self) -> bool:
        return self.__current_state != self.__target_state

    def is_reconf_par(self) -> bool:
        return self.__current_state.parallelism != self.__target_state.parallelism

    def is_reconf_transp(self) -> bool:
        return self.__current_state.transprecision != self.__target_state.transprecision
