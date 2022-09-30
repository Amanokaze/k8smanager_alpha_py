import os
import threading
import atexit
from engine.kubedata import Kubedata
from engine.processing import Processing
from engine.db import DB
from datetime import datetime
from psycopg2 import DatabaseError

LOGFILE_NAME = "manager"
DEBUG_FLAG = True

class Log:    
    def logfile_check(self):
        self._file_name = f"{LOGFILE_NAME}_{datetime.now().strftime('%y%m%d')}.log"
        
        if not os.path.isfile(self._file_name):
            file = open(self._file_name, "wt", encoding="utf8")
            file.close()

    def __init__(self):
        self.logfile_check()

    def write(self, log_type, message=None):
        self.logfile_check()
        statement = f"[{log_type}] {datetime.now().strftime('%m/%d %H:%M:%S')} - {message}"

        if DEBUG_FLAG or log_type == "Error":
            file = open(self._file_name, "a", encoding="utf8")
            file.write(statement)
            file.write("\n")
            file.close()

        if log_type == "Error":
            print(message)

class SYSTEM:
    def __init__(self):
        self._duration = 0
        self._network_metric = {
            "lastrealtimeperf": dict(),
            "nodeperf": dict(),
            "podperf": dict(),
            "podnet": dict()
        }

    def refresh_duration(self, interval):
        self._duration = self._duration + interval

    def get_duration(self):
        return self._duration

    def set_network_metric(self, net_type, item, data):
        self._network_metric[net_type][item] = data

    def get_network_metric(self, net_type, item):
        return self._network_metric[net_type][item] if item in self._network_metric[net_type] else None

class Engine:
    def __init__(self):
        self.log = Log()
        self.db = DB()
        self.system_var = SYSTEM()
        self.kubedata = Kubedata(self.log)

        self.start()

    def start(self):
        # onTune DB Connection을 위한 Connection Pool 역시 사전에 생성토록 함
        # Connection Pool이 정상 생성될 경우에만 최초 Thread를 가동토록 할 것
        try:
            self.db.create_connection()
            atexit.register(self.db.shutdown_connection_pool)
            threading.Thread(target=self.thread_func).start()

        except DatabaseError as e:
            self.log.write("ERROR", str(e))

    def thread_func(self):
        # 기본 구조는 KUBE_MGR_ST_INTERVAL 간격으로 Thread를 생성해서 기능을 수행하도록 함
        threading.Timer(self.kubedata.st_interval, self.thread_func).start()
        self.system_var.refresh_duration(self.kubedata.st_interval)
        
        # Stats API를 사용해서 데이터를 가져오는 부분
        self.kubedata.get_stats_api()

        # API 기본 정보를 가져오는 부분
        kube_basic_info = {
            "manager_name": self.db.get_basic_info("managername"),
            "manager_ip": self.db.get_basic_info("host")
        }
        
        # 데이터 가져오는 부분이 실패하면 onTune DB의 입력 의미가 없어지므로 Thread를 종료함
        if not self.kubedata.data_exist:
            return

        # onTune DB Data Processing
        # Processing Class는 Engine 선언부가 아닌 Thread 단위로 선언해서 생성 후 처리 완료 시 해제되는 형태로 되어야 함
        self.processing = Processing(self.log, self.db, self.system_var)
        self.processing.check_ontune_schema()
        self.processing.set_kube_data(self.kubedata, kube_basic_info)
        self.processing.update_reference_tables()
        self.processing.update_metric_tables()
