from time import sleep

from transscale.components.RuntimeContext import RuntimeContext
from transscale.components.ReconfigurationManager import ReconfigurationManager
from transscale.components.MeasurementsManager import MeasurementsManager
from transscale.components.ResourceManager import ResourceManager
from transscale.controllers.ParallelismController import ParallelismController
from transscale.controllers.TransprecisionController import TransprecisionController, convert_throughput

from transscale.utils.Config import Config
from transscale.utils.DefaultValues import DefaultValues, ConfigKeys as Key

import transscale.strategies.ReconfigurationStrategy as rs
from transscale.utils.Logger import Logger

script_name = "TRANSSCALE"

log = Logger()
log.info(f"{script_name}:: Starting auto-scaler...\n")

config = Config(log, "conf/transscale.conf")
debug = int(config.get(Key.DEBUG_LEVEL))
log.set_debug_level(debug)

measurements = MeasurementsManager(config, log)
reconf_manager = ReconfigurationManager(config, log)
resource_manager = ResourceManager(config, log)

par_contr = ParallelismController(config, log)
transp_contr = TransprecisionController(config, log)

context = RuntimeContext(config, log)

running = True

context.update_job_details()
context.update_state()
context.print_details()

while running:  # main loop, find a way to break it
    log.info("\n*******************************************************")
    if debug < 2:
        log.info(f"Warming up for {config.get(Key.MONITORING_WARMUP)} seconds")
        sleep(config.get_int(Key.MONITORING_WARMUP))

    # get it inside loop because after parallelism reconfiguration
    # the job is restarted, thus it gets a new id
    context.update_job_details()
    context.update_state()
    context.print_details()

    log.info(f"{script_name}:: Monitoring the throughput and back pressure every "
            f"{config.get(Key.MONITORING_INTERVAL)} seconds...")

    reconfigured = False

    while running and not reconfigured:  # monitoring loop
        sleep(config.get_int(Key.MONITORING_INTERVAL))
        log.info(f"\n{script_name}:: Periodic Monitoring...")

        context.update_job_runtime_metrics()
        context.print_job_runtime_metrics()

        context.update_state()

        # get source backpressure
        source_backpressure_ratio = context.get_backpressure()
        backpressure_ratio_percent = source_backpressure_ratio * 100

        # get source throughput
        input_rate = context.get_source_input_rate()

        if input_rate == 0:
            log.warning("No data detected. Should stop the monitoring?")

        elif backpressure_ratio_percent <= 50:
            log.info("\t Backpressure level is NOT HIGH")

            reconf_mode = reconf_manager.get_scaledown_params(context)[rs.RSP_METHOD]

            if reconf_mode == DefaultValues.Scaling.SCALE_PAR:
                context.set_target_state(
                    par=par_contr.scaledown(context, measurements),
                    transp=context.get_current_transp()
                )

            elif reconf_mode == DefaultValues.Scaling.SCALE_TRANSP:
                context.set_target_state(
                    par=context.get_current_par(),
                    transp=transp_contr.scaledown(context, measurements)
                )

        else:
            log.new_line()
            log.warning("Warning :: Back Pressure Level of Source is HIGH!!")

            log.info(f"Waiting to check again the backpressure after {config.get(Key.MONITORING_INTERVAL)} seconds...")
            sleep(config.get_int(Key.MONITORING_INTERVAL))

            context.update_job_runtime_metrics()
            context.print_job_runtime_metrics()

            # get source backpressure
            source_backpressure_ratio = context.get_backpressure()
            backpressure_ratio_percent = source_backpressure_ratio * 100

            if backpressure_ratio_percent > 50:
                log.info("Backpressure is still high and the system needs a reconfiguration...")

                # context.update_job_runtime_metrics()
                current_tput = context.get_operator_throughput()
                input_rate = context.get_source_input_rate()

                current_tput = convert_throughput(context, config.get(Key.SCALING_METHOD), current_tput)

                # context.print_job_runtime_metrics()

                scale_params = reconf_manager.get_scaleup_params(context)
                reconf_mode = scale_params[rs.RSP_METHOD]

                reconf_optimization = reconf_manager.get_scaling_optimization()

                log.debug(f"[DEBUG]\t Scale params: {scale_params}")
                log.debug(f"[DEBUG]\t Optimization: {reconf_optimization}")

                measurements.update_mst(context)
                nd_max = resource_manager.get_max_network_delay(context.get_current_par())
                measurements.update_nd(context, nd_max)

                if reconf_mode == DefaultValues.Scaling.SCALE_PAR:
                    context.set_target_state(
                        par=par_contr.scaleup(context, measurements, resource_manager, reconf_optimization),
                        transp=scale_params[rs.RSP_TRANSP] if rs.RSP_TRANSP in scale_params
                        else context.get_current_transp()
                    )

                elif reconf_mode == DefaultValues.Scaling.SCALE_TRANSP:
                    context.set_target_state(
                        par=scale_params[rs.RSP_PAR] if rs.RSP_PAR in scale_params
                        else context.get_current_par(),
                        transp=transp_contr.scaleup(context, measurements)
                    )

            else:
                log.info("Backpressure re-stabilized: no re-configuration needed")

        log.debugg(
            f"\t\t PAR -- current {context.get_current_par()} -- target {context.get_target_par()} "
            f"-- reconf {context.get_current_par() != context.get_target_par()}")
        log.debugg(
            f"\t\t TRANSP -- current {context.get_current_transp()} -- target {context.get_target_transp()} "
            f"-- reconf {context.get_current_transp() != context.get_target_transp()}")
        log.debugg(
            f"\t\t CTX -- reconf {context.is_reconf_required()} "
            f"-- par {context.is_reconf_par()} -- transp {context.is_reconf_transp()}")

        if context.is_reconf_required():
            log.new_line()
            if context.is_reconf_par():
                if not resource_manager.rescale_parallelism(context):
                    log.new_line()
                    log.info(f"{script_name}:: Error reconfiguring: closing auto-scaler")
                    running = False
                else:
                    context.update_job_details()

            if context.is_reconf_transp():
                if not resource_manager.rescale_transprecision(context):
                    log.info(f"{script_name}:: Error reconfiguring: closing auto-scaler")
                    running = False

            reconfigured = True
