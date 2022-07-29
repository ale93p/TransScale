from time import sleep
import subprocess as sp
import shlex as sx
from os.path import expanduser
import traceback

from transscale.components.RuntimeContext import RuntimeContext
from transscale.utils.Config import Config
from transscale.utils.DefaultValues import ConfigKeys as Key


class ResourceManager:
    class __Node:
        def __init__(self, site: str, node_id: str, delay: int = None):
            self.site = site
            self.id = node_id
            self.delay = delay
            
    def __init__(self, conf: Config):
        self.__debug = int(conf.get(Key.DEBUG_LEVEL))

        self.__nodes = self.get_nodes()
        self.__kube_ssh = "pico1"

        self.__redis_home = expanduser(conf.get(Key.REDIS_HOME))
        self.__redis_db_num = conf.get(Key.REDIS_DB)
        self.__redis_host = conf.get(Key.REDIS_HOST)
        self.__redis_port = conf.get(Key.REDIS_PORT)

        self.__flink_cmd = expanduser(conf.get(Key.FLINK_CMD))
        self.__job_path = expanduser(conf.get(Key.FLINK_JOB_PATH))

    def get_nodes(self) -> list[__Node]:
        clus_nodes = [self.__Node("", "", 0) for i in range(10)]
        clus_nodes[0] = self.__Node("Paris", "pico0")  # Master Node, no delay necessary
        clus_nodes[1] = self.__Node("Brussels", "pico1", 5)
        clus_nodes[2] = self.__Node("Amsterdam", "pico2", 10)
        clus_nodes[3] = self.__Node("London", "pico3", 20)
        clus_nodes[4] = self.__Node("Madrid", "pico4", 25)
        clus_nodes[5] = self.__Node("Rome", "pico5", 40)
        clus_nodes[6] = self.__Node("Stockholm", "pico6", 50)
        clus_nodes[7] = self.__Node("Helsinki", "pico7", 55)  # One node is broken
        clus_nodes[8] = self.__Node("Berlin", "pico8", 70)  # Job Manager
        clus_nodes[9] = self.__Node("Vienna", "pico9", 90)  # Minio, Prometheus and Graphana

        return clus_nodes

    def get_max_network_delay(self, par: int) -> int:
        return self.__nodes[par].delay

    def rescale_parallelism(self, context: RuntimeContext) -> bool:
        cluster_ip = context.get_cluster_ip()
        job_id = context.get_job_id()
        target_par = context.get_target_par()

        print("\n*******************************************************")
        print(f"[RES_MNGR] Re-configuring PARALLELISM")
        print(f"\tCurrent Parallelism: {context.get_current_par()}")
        print(f"\tTarget Parallelism: {target_par}")

        try:
            # TODO: Flink checkpointing

            print(f"[RES_MNGR] Stopping Flink for re-configuration...")
            stop_cmd = f"{self.__flink_cmd} cancel -m {cluster_ip} {job_id}"
            if self.__debug > 0:
                print(f"[DEBUG]\t Running command: {stop_cmd}")
            proc = sp.run(sx.split(stop_cmd), capture_output=True, check=True)
            if self.__debug > 0:
                print(f"[DEBUG]\t Exit code: {proc.returncode}")
            sleep(15)  # TODO: parametrize sleeping time

            print()
            print(f"[RES_MNGR] Re-scaling number of task managers...")
            proc = self.__rescale_kube(context)
            proc.check_returncode()
            sleep(5) # TODO: parametrize sleeping time

            print()
            print(f"[RES_MNGR] Resuming Flink with new configuration...")
            run_cmd = f"{self.__flink_cmd} run -d -m {cluster_ip} -p {target_par} -j {self.__job_path}"
            if self.__debug > 0:
                print(f"[DEBUG]\t Running command: {run_cmd}")
            proc = sp.run(sx.split(run_cmd), capture_output=True, check=True)
            if self.__debug > 0:
                print(f"[DEBUG]\t Exit code: {proc.returncode}")

        except sp.CalledProcessError as e:
            print()
            traceback.print_exception(e)
            print()
            print("Process returns:")
            print(f"\t Exit code: {e.returncode}")
            print("\nSTDOUT")
            print(e.stdout)
            print("\nSTDERR")
            print(e.stderr)
            return False

        return True

    def rescale_transprecision(self, context: RuntimeContext) -> bool:
        target_transp = context.get_target_transp()

        print("\n*******************************************************")
        print(f"[RES_MNGR] Re-configuring TRANSPRECISION")
        print(f"\tCurrent Transprecision: {context.get_current_transp()}")
        print(f"\tTarget Transprecision: {target_transp}")

        print(f"[RES_MNGR] Changing Transprecision Level...")
        redis_cmd = f"{self.__redis_home}/src/redis-cli -n {self.__redis_db_num} " \
                    f"-h {self.__redis_host} -p {self.__redis_port} " \
                    f"set transprecision_level {target_transp}"
        sp.run(sx.split(redis_cmd), capture_output=True, check=True)

        while context.get_current_transp() != target_transp:
            sleep(5)  # TODO: parametrize sleeping time
            context.update_state()
            if self.__debug > 0:
                print(f"[DEBUG]\tCurrent Transprecision: {context.get_current_transp()}")
                print(f"[DEBUG]\tTarget Transprecision: {target_transp}")

        return True

    def __rescale_kube(self, context: RuntimeContext) -> sp.CompletedProcess:
        current_par = context.get_current_par()
        target_par = context.get_target_par()

        increase_cmd = f"sshpass -p pico ssh guru@{self.__kube_ssh} " \
                       f"kubectl scale --replicas={target_par} deployment/flink-taskmanager"
        reset_cmd = f"sshpass -p pico ssh guru@{self.__kube_ssh} " \
                    f"kubectl scale --replicas=0 deployment/flink-taskmanager"

        proc = None
        try:
            if target_par > current_par:
                if self.__debug > 0:
                    print(f"[DEBUG]\t Running command: {increase_cmd}")
                proc = sp.run(sx.split(increase_cmd), capture_output=True, check=True)
                if self.__debug > 0:
                    print(f"[DEBUG]\t Exit code: {proc.returncode}")
            else:
                if self.__debug > 0:
                    print(f"[DEBUG]\t Running command: {reset_cmd}")
                proc = sp.run(sx.split(reset_cmd), capture_output=True, check=True)
                if self.__debug > 0:
                    print(f"[DEBUG]\t Exit code: {proc.returncode}")
                sleep(15)
                if self.__debug > 0:
                    print(f"[DEBUG]\t Running command: {increase_cmd}")
                proc = sp.run(sx.split(increase_cmd), capture_output=True, check=True)
                if self.__debug > 0:
                    print(f"[DEBUG]\t Exit code: {proc.returncode}")
        except sp.CalledProcessError:
            return proc

        return proc
