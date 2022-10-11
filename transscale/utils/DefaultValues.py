from os.path import join


class ConfigKeys:
    MONITORING_INTERVAL = "sys.monitoring.interval"
    MONITORING_WARMUP = "sys.monitoring.warmup"
    MAX_PAR = "sys.max.par"
    MAX_TRANSP = "sys.max.transp"

    SCALING_STRATEGY = "scaling.strategy.module"
    SCALING_OPTIMIZATION = "scaling.strategy.optimization"
    SCALING_METHOD = "scaling.transprecision.method"
    TPUT_THRESHOLD_TRANSP = "scaling.transprecision.threshold"
    TPUT_THRESHOLD_PAR = "scaling.parallelism.threshold"
    TPUT_THRESHOLD_COMBO = "scaling.combined.threshold"

    FLINK_HOST = "flink.host"
    FLINK_PORT = "flink.port"
    FLINK_CMD = "flink.cmd"
    FLINK_JOB_PATH = "flink.job.path"

    REDIS_HOME = "redis.home"
    REDIS_DB = "redis.db.num"
    REDIS_HOST = "redis.host"
    REDIS_PORT = "redis.port"

    DEBUG_LEVEL = "debug.level"


class DefaultValues:
    class System:
        CONF_PATH = "conf/transscale.conf"

        class Debug:
            DISABLED = 0
            LEVEL_1 = 1

            level = DISABLED

        class Monitoring:
            interval = 60
            warmup = 300

        class Environment:
            max_par = 6
            max_transp = 3

    class Scaling:
        SCALE_UP = 0
        SCALE_DOWN = 1

        SCALE_PAR = 0
        SCALE_TRANSP = 1
        COMBO_SCALE = 2

        class Strategy:
            class ScaleOptimization:
                SINGLE_CONTROLLER = False
                PREDICTION_MATRIX = True

            module = "transscale.strategies.ParallelismOnly"
            optimization_method = ScaleOptimization.SINGLE_CONTROLLER

        class Transprecision:
            SWITCH_BASE = "switching_base"
            SAMPLING_DIRECT = "sampling_direct"

            method = SAMPLING_DIRECT
            threshold = 15

        class Parallelism:
            threshold = 30

        class Combined:
            threshold = 30

    class Redis:
        home = join("~", "redis-6.2.6")
        db_num = "15"
        host = "localhost"
        port = "6379"

    class Flink:
        host = "localhost"
        port = "8081"
        command = "flink"
        job_path = "flink-job.jar"
