from numpy import zeros
from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key
import traceback

from transscale.utils.Logger import Logger


class MeasurementsManager:

    def __init__(self, conf: Config, log: Logger):
        self.__mst_by_par = zeros([conf.get_int(Key.MAX_PAR) + 1, conf.get_int(Key.MAX_TRANSP) + 1])
        self.__mst_by_transp = self.__mst_by_par.transpose()
        self.__ndmax = zeros([conf.get_int(Key.MAX_PAR) + 1])
        self.__debug = conf.get(Key.DEBUG_LEVEL)
        self.__log = log

    def get_measurements(self, par: int = None, transp: int = None) -> int | list[int]:
        if par and transp:
            return self.__mst_by_par[par, transp]
        elif par and not transp:
            return self.__mst_by_par[par]
        elif not par and transp:
            return self.__mst_by_transp[transp]
        else:
            raise ValueError("At least one of the parameters must be defined")

    # Called by the parallelism controller
    # Returns the number of measurements at different parallelism levels for the current transp level
    def get_measurements_num_par(self, transp: int) -> int:
        return len([x for x in self.__mst_by_transp[transp] if x > 0])

    # Called by the transprecision controller
    # Returns the number of measurements at different transprecision levels for the current par level
    def get_measurements_num_transp(self, par: int) -> int:
        return len([x for x in self.__mst_by_par[par] if x > 0])

    def get_network_distance(self) -> list[int]:
        return list(self.__ndmax)

    def get_ndmax(self, par: int) -> int:
        return self.__ndmax[par]

    def update_mst(self, context: RuntimeContext) -> None:
        par = context.get_current_par()
        transp = context.get_current_transp()
        mst = context.get_source_input_rate()
        try:
            self.__mst_by_par[par, transp] = mst
            self.__mst_by_transp[transp, par] = mst
        except IndexError as err:
            self.__log.error(str(traceback.format_exception(None, err, err.__traceback__)))
            self.__log.info(f"par is {par} type {type(par)}")
            self.__log.info(f"par is {transp} type {type(transp)}")
            self.__log.info(f"par is {mst} type {type(mst)}")
            quit(-1)

    def update_nd(self, context: RuntimeContext, nd_max: int) -> None:
        par = context.get_current_par()
        self.__ndmax[par] = nd_max


