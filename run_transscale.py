from time import sleep
import argparse

from transscale.components.RuntimeContext import RuntimeContext
from transscale.components.MeasurementsManager import MeasurementsManager
from transscale.components.ResourceManager import ResourceManager
from transscale.controllers.CombinedController import CombinedController
from transscale.controllers.TransprecisionController import convert_throughput
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key
from transscale.utils.Logger import Logger


def debug_status(ctx: RuntimeContext) -> None:
    log.debugg(
        f"\t\t PAR -- current {ctx.get_current_par()} -- target {ctx.get_target_par()} "
        f"-- reconf {ctx.get_current_par() != ctx.get_target_par()}")
    log.debugg(
        f"\t\t TRANSP -- current {ctx.get_current_transp()} -- target {ctx.get_target_transp()} "
        f"-- reconf {ctx.get_current_transp() != ctx.get_target_transp()}")
    log.debugg(
        f"\t\t CTX -- reconf {ctx.is_reconf_required()} "
        f"-- par {ctx.is_reconf_par()} -- transp {ctx.is_reconf_transp()}")


if __name__ == "__main__":
    script_name = "TRANSSCALE"

    log = Logger()
    log.info(f"{script_name}:: Starting auto-scaler...\n")

    config_file = "/home/apagliar/Workspace/platforms/Gesscale-transprecision-py/conf/transscale.conf"

    parser = argparse.ArgumentParser(description='Transscale auto-scaler for Flink data stream processing')
    parser.add_argument('-c', '--conf', dest='conf_file', action='store',
                        default=config_file,
                        help='sum the integers (default: find the max)')

    args = parser.parse_args()
    config_file = args.conf_file

    config = Config(log, config_file)
    debug = int(config.get(Key.DEBUG_LEVEL))
    log.set_debug_level(debug)

    measurements = MeasurementsManager(config, log)
    resource_manager = ResourceManager(config, log)
    combo_contr = CombinedController(config, log)

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

                if context.get_current_par() == 1 and context.get_current_transp() == 1:
                    log.warning("Resources are already at min level. Cannot scale-down.")

                else:
                    target_par, target_transp = combo_contr.scaledown(context)
                    context.set_target_state(par=target_par, transp=target_transp)

            else:
                log.new_line()
                log.warning("Back Pressure Level of Source is HIGH!!")

                if context.get_current_par() == context.get_max_par() \
                        and context.get_current_transp() == context.get_max_transp():
                    log.warning("Resources are already at max level. Cannot scale-up.")

                else:

                    log.info(
                        f"Waiting to check again the backpressure after {config.get(Key.MONITORING_INTERVAL)} seconds...")
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

                        measurements.update_mst(context)
                        nd_max = resource_manager.get_max_network_delay(context.get_current_par())
                        measurements.update_nd(context, nd_max)

                        target_par, target_transp = combo_contr.scaleup(context, measurements)

                        context.set_target_state(par=target_par, transp=target_transp)

                    else:
                        log.info("Backpressure re-stabilized: no re-configuration needed")

            debug_status(context)

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
