import os
from re import X
import time
import json
import threading
import csv
import atexit
import stmt
from db import DB
from psycopg2 import DatabaseError
from datetime import date, datetime, timedelta
from kubernetes import client

DEFAULT_INFO = {
    "KUBE_HOST": "http://localhost:6443",
    "KUBE_API_KEY": None,
    "KUBE_MGR_ST_INTERVAL": 10,
    "KUBE_MGR_LT_INTERVAL": 600,
    "KUBE_CLUSTER_NAME": "kubernetes",
}

KUBE_HOST = os.environ["KUBE_HOST"] if "KUBE_HOST" in os.environ else DEFAULT_INFO["KUBE_HOST"]
KUBE_API_KEY = os.environ["KUBE_API_KEY"] if "KUBE_API_KEY" in os.environ else DEFAULT_INFO["KUBE_API_KEY"]
KUBE_MGR_ST_INTERVAL = int(os.environ["KUBE_MGR_ST_INTERVAL"])  if "KUBE_MGR_ST_INTERVAL" in os.environ else DEFAULT_INFO["KUBE_MGR_ST_INTERVAL"]
KUBE_MGR_LT_INTERVAL = int(os.environ["KUBE_MGR_LT_INTERVAL"])  if "KUBE_MGR_LT_INTERVAL" in os.environ else DEFAULT_INFO["KUBE_MGR_LT_INTERVAL"]
KUBE_CLUSTER_NAME = os.environ["KUBE_CLUSTER_NAME"] if "KUBE_CLUSTER_NAME" in os.environ else DEFAULT_INFO["KUBE_CLUSTER_NAME"]
#KUBE_MGR_ST_INTERVAL = DEFAULT_INFO["KUBE_MGR_ST_INTERVAL"]

LOGFILE_NAME = "manager"
ONE_DAY_SECONDS = 86400

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

    def refresh_duration(self):
        self._duration = self._duration + KUBE_MGR_ST_INTERVAL

    def get_duration(self):
        return self._duration

    def set_network_metric(self, net_type, item, data):
        self._network_metric[net_type][item] = data

    def get_network_metric(self, net_type, item):
        return self._network_metric[net_type][item] if item in self._network_metric[net_type] else None

class Engine:
    def __init__(self):
        # Kube Cluster 접속은 최초 1회만 이루어지며, Thread 별로 접속을 보장하지 않음
        self.kube_cfg = client.Configuration()
        self.kube_cfg.api_key['authorization'] = KUBE_API_KEY
        self.kube_cfg.api_key_prefix['authorization'] = 'Bearer'
        self.kube_cfg.host = KUBE_HOST
        self.kube_cfg.verify_ssl = True
        self.kube_cfg.ssl_ca_cert = 'ca.crt'

        self.log = Log()
        self.db = DB()
        self.system_var = SYSTEM()

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
        threading.Timer(KUBE_MGR_ST_INTERVAL, self.thread_func).start()
        self.system_var.refresh_duration()
        
        # API 기본 정보를 가져오는 부분
        # Cluster name 정보는 API를 통해서 가져올 수 없는 것으로 최종 확인되었음
        api_version_info = client.CoreApi(client.ApiClient(self.kube_cfg)).get_api_versions()
        kube_basic_info = {
            "manager_name": self.db.get_basic_info("managername"),
            "manager_ip": self.db.get_basic_info("host"),
            "cluster_name": KUBE_CLUSTER_NAME,
            "cluster_address": api_version_info.server_address_by_client_cid_rs[0].server_address.split(":")[0]
        }
        
        # Stats API를 사용해서 데이터를 가져오는 부분
        core_api = client.CoreV1Api(client.ApiClient(self.kube_cfg))
        apps_api = client.AppsV1Api(client.ApiClient(self.kube_cfg))

        (ndata, nmdata, pmdata) = self.get_kube_node_data(core_api)
        pdata = self.get_kube_pod_data(core_api)
        svcdata = self.get_kube_svc_data(core_api)
        dsdata = self.get_kube_ds_data(apps_api)
        rsdata = self.get_kube_rs_data(apps_api)
        deploydata = self.get_kube_deploy_data(apps_api)
        stsdata = self.get_kube_sts_data(apps_api)

        kube_data = {
            "node": ndata,
            "pod": pdata,
            "node_metric": nmdata,
            "pod_metric": pmdata,
            "ns": self.get_kube_ns_data(core_api),
            "svc": svcdata,
            "deploy": deploydata,
            "sts": stsdata,
            "ds": dsdata,
            "rs": rsdata
        }

        # 데이터 가져오는 부분이 실패하면 onTune DB의 입력 의미가 없어지므로 Thread를 종료함
        if not kube_data["node"] or not kube_data["ns"]:
            return

        # onTune DB Schema Check 및 데이터를 입력하는 부분
        schema_obj = self.check_ontune_schema()
        result = self.put_kube_data_to_ontune(kube_basic_info, kube_data, schema_obj)

        if result:
            pass

    def change_quantity_unit(self, value):
        try:
            return int(value)
        except ValueError:
            if "Ki" in value:
                return int(value[:-2]) * 1024
            elif "Mi" in value:
                return int(value[:-2]) * 1048576
            elif "Gi" in value:
                return int(value[:-2]) * 1073741824
            elif "m" in value:
                return int(value[:-1]) * 0.001
            elif "e" in value:
                return int(value[:-2]) * (10 ** int(value[-1]))
            elif "k" in value:
                return int(value[:-1]) * 1000
            elif "M" in value:
                return int(value[:-1]) * 1000000
            elif "G" in value:
                return int(value[:-1]) * 1000000000
            else:
                return 0

    def dict_to_str(self, data):
        return ",".join(list(f"{k}={v}" for (k,v) in data.items()))

    def dict_port_to_str(self, ports):
        port_str = list((f"{x.port}" if x.port == x.target_port else f"{x.port}:{x.target_port}") + f"/{x.protocol}" for x in ports)
        return ",".join(port_str)

    def input_tableinfo(self, name, cursor, conn, ontunetime=0):
        ontunetime = self.get_ontunetime(cursor) if ontunetime == 0 else ontunetime
        cursor.execute(stmt.SELECT_TABLEINFO_TABLENAME.format(name))
        result = cursor.fetchone()

        exec_stmt = stmt.UPDATE_TABLEINFO.format(ontunetime, name) if result[0] == 1 else stmt.INSERT_TABLEINFO.format(ontunetime, name)
        cursor.execute(exec_stmt)
        conn.commit()
        self.log.write("PUT", f"{name} data is added in kubetableinfo table.")

    def insert_columns_ref(self, schema, obj_name):
        return ",".join(list(y[0] for y in list(filter(lambda x: 'PRIMARY KEY' not in x, schema["reference"][obj_name]))))

    def insert_columns_metric(self, schema, obj_name):
        metric_list = list(schema["reference"][obj_name]) if obj_name == "kubelastrealtimeperf" else list(schema["metric"][obj_name])
        return ",".join(list(y[0] for y in metric_list))

    def insert_values(self, data):
        return ",".join(list(f"'{x}'" for x in data))

    def select_average_columns(self, schema, obj_name, key_columns):
        not_averaged_columns = key_columns + ['_ontunetime','_agenttime']
        metric_list = list(x[0] for x in schema["metric"][obj_name] if x[0] not in not_averaged_columns)
        return ",".join(key_columns + list(f"round(avg({x}))::int8 {x}" for x in metric_list))

    def update_values(self, schema, obj_name, data):
        not_included_column_list = ("_managerid","_clusterid")
        column_list = list(y[0] for y in list(filter(lambda x: 'PRIMARY KEY' not in x and x[0] not in not_included_column_list, schema["reference"][obj_name])))
        value_list = list(data.values())
        stmt_list = list(f"{column_list[i]}='{value_list[i]}'" for i in range(len(column_list)))

        return ",".join(stmt_list)

    def get_ontunetime(self, cursor):
        cursor.execute(stmt.SELECT_ONTUNEINFO)
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_agenttime(self, cursor):
        cursor.execute(stmt.SELECT_ONTUNEINFO_BIAS)
        result = cursor.fetchone()
        return result[0]+(result[1]*60) if result else 0

    def svtime_to_timestampz(self, svtime):
        tz_interval = ONE_DAY_SECONDS - int(time.mktime(time.gmtime(ONE_DAY_SECONDS)))
        return int(time.mktime((datetime.strptime(svtime, '%Y-%m-%dT%H:%M:%SZ') + timedelta(seconds=tz_interval)).timetuple()))

    def calculate(self, cal_type, values):
        NANO_VALUES = 10 ** 9
        KILO_VALUES = 1024
        MEGA_VALUES = KILO_VALUES ** 2
        PERCENT_VALUE = 100
        DUPLICATE_VALUE = 100
        
        if cal_type == "cpu_usage_percent":
            # Reference: https://github.com/kubernetes/heapster/issues/650#issuecomment-147795824
            vals = list(int(x) if x else 0 for x in values)
            return round(vals[0] / (vals[1] * NANO_VALUES) * PERCENT_VALUE)
        elif cal_type[:6] == "memory":
            wsbytes = int(values["workingSetBytes"]) if "workingSetBytes" in values else 0
            avbbytes = int(values["availableBytes"]) if "availableBytes" in values else 0
            rssbytes = int(values["rssBytes"]) if "rssBytes" in values else 0
            limitbytes = wsbytes + avbbytes

            if cal_type == "memory_used_percent":
                # Reference: WSBytes / (WSBytes + Available = Limit) (Now not used UsageBytes)
                # WSBytes: The amount of working set memory. This includes recently accessed memory, dirty memory, and kernel memory. WorkingSetBytes is <= UsageBytes
                # UsageBytes: Total memory in use. This includes all memory regardless of when it was accessed.        
                return round(wsbytes / limitbytes * PERCENT_VALUE * DUPLICATE_VALUE) if limitbytes > 0 else 0
            elif cal_type == "memory_swap_percent":
                # Reference: RSSBytes / (WSBytes + Available = Limit)
                # RSSBytes: The amount of anonymous and swap cache memory (includes transparent hugepages)
                return round(rssbytes / limitbytes * PERCENT_VALUE * DUPLICATE_VALUE) if limitbytes > 0 else 0
            elif cal_type == "memory_size":
                return limitbytes
            else:
                return 0
        elif cal_type == "network":
            return round(int(values[0]) / KILO_VALUES)
        elif cal_type[:2] == "fs":
            totalbytes = int(values["capacityBytes"]) if "capacityBytes" in values else 0
            freebytes = int(values["availableBytes"]) if "availableBytes" in values else 0
            usedbytes = int(values["usedBytes"]) if "usedBytes" in values else 0

            itotalbytes = int(values["inodes"]) if "inodes" in values else 0
            ifreebytes = int(values["inodesFree"]) if "inodesFree" in values else 0
            iusedbytes = int(values["inodesUsed"]) if "inodesUsed" in values else 0

            if cal_type == "fs_usage_percent":
                # Reference: UsedBytes / TotalBytes
                # UsedBytes - This may differ from the total bytes used on the filesystem and may not equal CapacityBytes - AvailableBytes.
                return round(usedbytes / totalbytes * PERCENT_VALUE) if totalbytes > 0 else 0
            elif cal_type == "fs_total_size":
                return round(totalbytes / MEGA_VALUES)
            elif cal_type == "fs_free_size":
                return round(freebytes / MEGA_VALUES)        
            elif cal_type == "fs_inode_usage_percent":
                # Reference: inodesUsed / inodes
                return round(iusedbytes / itotalbytes * PERCENT_VALUE) if itotalbytes > 0 else 0
            elif cal_type == "fs_inode_total_size":
                return round(itotalbytes / KILO_VALUES)
            elif cal_type == "fs_inode_free_size":
                return round(ifreebytes / KILO_VALUES)
            else:
                return 0
        else:
            return 0

    def get_kube_node_data(self, api):
        try:
            nodes = api.list_node()
            node_data = dict()
            node_metric_data = dict()
            pod_metric_data = dict()
            
            for node in nodes.items:
                nodename = node.metadata.name

                node_data[nodename] = {
                    "uid": node.metadata.uid,
                    "name": nodename,
                    "nameext": nodename,
                    "enabled": 1,
                    "state": 1,
                    "connected": 1,
                    "starttime": self.svtime_to_timestampz(node.metadata.creation_timestamp),
                    "kernelversion": node.status.node_info.kernel_version,
                    "osimage": node.status.node_info.os_image,
                    "osname": node.status.node_info.operating_system,
                    "containerruntimever": node.status.node_info.container_runtime_version,
                    "kubeletver": node.status.node_info.kubelet_version,
                    "kubeproxyver": node.status.node_info.kube_proxy_version,
                    "cpuarch": node.status.node_info.architecture,
                    "cpucount": node.status.capacity["cpu"],
                    "ephemeralstorage": self.change_quantity_unit(node.status.capacity["ephemeral-storage"]),
                    "memorysize": self.change_quantity_unit(node.status.capacity["memory"]),
                    "pods": node.status.capacity["pods"],
                    "ip": node.status.addresses[0].address
                }

                try:
                    node_stats = api.connect_get_node_proxy_with_path(nodename, "stats/summary")
                    node_stats_json = json.loads(node_stats.replace("'",'"'))
                    node_metric_data[nodename] = node_stats_json['node']
                    pod_metric_data[nodename] = node_stats_json['pods']
                except client.rest.ApiException as e:
                    node_metric_data[nodename] = dict()
                    pod_metric_data[nodename] = dict()
                    node_data[nodename]["state"] = 0
                    node_data[nodename]["connected"] = 0

            self.log.write("GET", "Kube Node Data Import is completed.")

            return (node_data, node_metric_data, pod_metric_data)

        except Exception as e:
            self.log.write("Error", str(e))
            return (False, False, False)

    def get_kube_pod_data(self, api):
        try:
            pods = api.list_pod_for_all_namespaces()
            pod_data = dict()
            
            for pod in pods.items:
                pod_data[pod.metadata.uid] = {
                    "nodeid": 0,
                    "nsid": 0,
                    "uid": pod.metadata.uid,
                    "name": pod.metadata.name,
                    "starttime": self.svtime_to_timestampz(pod.metadata.creation_timestamp),
                    "restartpolicy": pod.metadata.restart_policy, 
                    "serviceaccount": pod.metadata.service_account,
                    "status": pod.status.phase,
                    "hostip": pod.status.host_ip,
                    "podip": pod.status_pod_ip,
                    "restartcount": sum(list(x.restart_count for x in pod.status.container_statuses)),
                    "restarttime": max(list(self.svtime_to_timestampz(x.state.running.started_at) for x in pod.status.container_statuses)),
                    "nodename": pod.metadata.node_name,
                    "nsname": pod.metadata.namespace,
                    "refkind": pod.metadata.owner_references[0].kind if "owner_references" in pod.metadata else "",
                    "refid": 0,
                    "refuid": pod.metadata.owner_references[0].uid if "owner_references" in pod.metadata else 0,
                    "containers": list() if "owner_references" in pod.metadata or pod.metadata.owner_references or pod.metadata.owner_references[0].kind != 'Node' else list({
                        "kind": "Pod",
                        "name": x.name,
                        "image": x.image,
                        "ports": str(x.ports) if "ports" in x else "",
                        "env": str(x.env) if "env" in x else "",
                        "resources": str(x.resources) if "resources" in x else "",
                        "volumemounts": str(x.volume_mounts) if "volume_mounts" in x else ""
                    } for x in pod.spec.containers)
                }

            self.log.write("GET", "Kube Pod Data Import is completed.")

            return pod_data

        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def get_kube_ns_data(self, api):
        try:
            nslist = api.list_namespace()
            return list({'name': x.metadata.name, 'status': x.status.phase} for x in nslist.items)
        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def get_kube_svc_data(self, api):
        try:
            services = api.list_service_for_all_namespaces()
            svc_data = dict()

            for svc in services.items:
                svc_data[svc.metadata.uid] = {
                    "nsid": 0,
                    "name": svc.metadata.name,
                    "uid": svc.metadata.uid,
                    "starttime": self.svtime_to_timestampz(svc.metadata.creation_timestamp),
                    "servicetype": svc.spec.type,
                    "clusterip": svc.spec.cluster_ip,
                    "ports": self.dict_port_to_str(svc.spec.ports),
                    "selector": self.dict_to_str(svc.spec.selector),
                    "nsname": svc.metadata.namespace
                }

            return svc_data                
        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def get_kube_ds_data(self, api):
        try:
            daemonsets = api.list_daemon_set_for_all_namespaces()
            ds_data = dict()

            for ds in daemonsets.items():
                ds_data[ds.metadata.uid] = {
                    "nsid": 0,
                    "name": ds.metadata.name,
                    "uid": ds.metadata.uid,
                    "starttime": self.svtime_to_timestampz(ds.metadata.creation_timestamp),
                    "serviceaccount": ds.spec.template.spec.service_account,
                    "current": ds.status.current_number_scheduled,
                    "desired": ds.status.desired_number_scheduled,
                    "ready": ds.status.number_ready,
                    "updated": ds.status.updated_number_scheduled,
                    "available": ds.status.number_available,
                    "selector": self.dict_to_str(ds.spec.selector.match_labels),
                    "nsname": ds.metadata.namespace,
                    "containers": list({
                        "kind": "DaemonSet",
                        "name": x.name,
                        "image": x.image,
                        "ports": str(x.ports) if "ports" in x else "",
                        "env": str(x.env) if "env" in x else "",
                        "resources": str(x.resources) if "resources" in x else "",
                        "volumemounts": str(x.volume_mounts) if "volume_mounts" in x else ""
                    } for x in ds.spec.containers)
                }

            return ds_data
        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def get_kube_rs_data(self, api):
        try:
            replicasets = api.list_replica_set_for_all_namespaces()
            rs_data = dict()

            for rs in replicasets.items:
                rs_data[rs.metadata.uid] = {
                    "nsid": 0,
                    "name": rs.metadata.name,
                    "uid": rs.metadata.uid,
                    "starttime": self.svtime_to_timestampz(rs.metadata.creation_timestamp),
                    "replicas": rs.spec.status.replicas,
                    "fullylabeledrs": rs.spec.status.fully_labeled_replicas,
                    "readyrs": rs.spec.status.ready_replicas,
                    "availablers": rs.spec.status.available_replicas,
                    "observedgen": rs.spec.status.observed_generation,
                    "selector": self.dict_to_str(rs.spec.selector.match_labels),
                    "refkind": rs.metadata.owner_references[0].kind if "owner_references" in rs.metadata else "",
                    "refid": 0,                    
                    "refuid": rs.metadata.owner_references[0].uid if "owner_references" in rs.metadata else 0,
                    "nsname": rs.metadata.namespace,
                    "containers": list() if "owner_references" in rs.metadata else list({
                        "kind": "ReplicaSet",
                        "name": x.name,
                        "image": x.image,
                        "ports": str(x.ports) if "ports" in x else "",
                        "env": str(x.env) if "env" in x else "",
                        "resources": str(x.resources) if "resources" in x else "",
                        "volumemounts": str(x.volume_mounts) if "volume_mounts" in x else ""
                    } for x in rs.spec.containers)
                }

            return rs_data                
        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def get_kube_deploy_data(self, api):
        try:
            deployments = api.list_deployment_for_all_namespaces()
            deploy_data = dict()

            for deploy in deployments.items:
                deploy_data[deploy.metadata.uid] = {
                    "nsid": 0,
                    "name": deploy.metadata.name,
                    "uid": deploy.metadata.uid,
                    "starttime": self.svtime_to_timestampz(deploy.metadata.creation_timestamp),
                    "serviceaccount": deploy.spec.template.spec.service_account,
                    "replicas": deploy.spec.status.replicas,
                    "updatedrs": deploy.spec.status.updated_replicas,
                    "readyrs": deploy.spec.status.ready_replicas,
                    "availablers": deploy.spec.status.available_replicas,
                    "observedgen": deploy.spec.status.observed_generation,
                    "selector": self.dict_to_str(deploy.spec.selector.match_labels),
                    "nsname": deploy.metadata.namespace,
                    "containers": list({
                        "kind": "Deployment",
                        "name": x.name,
                        "image": x.image,
                        "ports": str(x.ports) if "ports" in x else "",
                        "env": str(x.env) if "env" in x else "",
                        "resources": str(x.resources) if "resources" in x else "",
                        "volumemounts": str(x.volume_mounts) if "volume_mounts" in x else ""
                    } for x in deploy.spec.containers)
                }

            return deploy_data                
        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def get_kube_sts_data(self, api):
        try:
            statefulsets = api.list_stateful_set_for_all_namespaces()
            sts_data = dict()

            for sts in statefulsets.items:
                sts_data[sts.metadata.uid] = {
                    "nsid": 0,
                    "name": sts.metadata.name,
                    "uid": sts.metadata.uid,
                    "starttime": self.svtime_to_timestampz(sts.metadata.creation_timestamp),
                    "serviceaccount": sts.spec.template.spec.service_account,
                    "replicas": sts.spec.status.replicas,
                    "readyrs": sts.spec.status.ready_replicas,
                    "availablers": sts.spec.status.available_replicas,
                    "selector": self.dict_to_str(sts.spec.selector.match_labels),
                    "nsname": sts.metadata.namespace,
                    "containers": list({
                        "kind": "StatefulSet",
                        "name": x.name,
                        "image": x.image,
                        "ports": str(x.ports) if "ports" in x else "",
                        "env": str(x.env) if "env" in x else "",
                        "resources": str(x.resources) if "resources" in x else "",
                        "volumemounts": str(x.volume_mounts) if "volume_mounts" in x else ""
                    } for x in sts.spec.containers)
                }

            return sts_data                
        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def check_ontune_schema(self):
        # Load onTune Schema
        schema_object = {
            "reference": dict(),
            "metric": dict(),
            "index": dict()
        }
        with open('schema.csv', 'r', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            for row in reader:
                (schema_type, obj_name) = row[:2]

                if obj_name not in schema_object[schema_type]:
                    schema_object[schema_type][obj_name] = list()

                properties = list(filter(lambda x: x != "", row[2:]))
                schema_object[schema_type][obj_name].append(properties)

        with self.db.get_resource_rdb() as (cursor, _, conn):
            # Check Reference Tables
            for obj_name in schema_object["reference"]:
                properties = schema_object["reference"][obj_name]
                cursor.execute(stmt.SELECT_PG_TABLES_TABLENAME_COUNT_REF.format(obj_name))
                result = cursor.fetchone()

                if result[0] == 1:
                    self.log.write("GET", f"Reference table {obj_name} is checked.")
                else:
                    self.log.write("GET", f"Reference table {obj_name} doesn't exist. now it will be created.")
                    creation_prefix = "create table if not exists"
                    column_properties = ",".join(list(" ".join(x) for x in properties))
                    table_creation_statement = f"{creation_prefix} {obj_name} ({column_properties});"
                    cursor.execute(table_creation_statement)
                    conn.commit()
                    self.log.write("PUT", f"Reference table {obj_name} creation is completed.")

                    self.input_tableinfo(obj_name, cursor, conn)
            
            # Check Metric Tables
            for obj_name in schema_object["metric"]:
                properties = schema_object["metric"][obj_name]
                table_postfix = f"_{datetime.now().strftime('%y%m%d')}00"
                cursor.execute(stmt.SELECT_PG_TABLES_TABLENAME_COUNT_MET.format(obj_name, table_postfix))
                result = cursor.fetchone()

                if result[0] == 2:
                    self.log.write("GET", f"Realtime/avg{obj_name}{table_postfix} metric tables are checked.")
                else:
                    # Metric Table Creation
                    self.log.write("GET", f"Realtime/avg{obj_name}{table_postfix} metric tables doesn't exist. now they will be created.")
                    creation_prefix = "create table if not exists"
                    column_properties = ",".join(list(" ".join(x) for x in properties))

                    for table_prefix in ('realtime','avg'):
                        full_table_name = f"{table_prefix}{obj_name}{table_postfix}"
                        table_creation_statement = f"{creation_prefix} {full_table_name} ({column_properties});"
                        cursor.execute(table_creation_statement)
                        conn.commit()
                        self.log.write("PUT", f"Metric table {full_table_name} creation is completed.")

                        # Metric Table Index Creation
                        index_properties = ",".join(schema_object["index"][obj_name][0])
                        index_creation_statement = f"create index if not exists i{full_table_name} on public.{full_table_name} using btree ({index_properties});"
                        cursor.execute(index_creation_statement)
                        conn.commit()
                        self.log.write("PUT", f"Metric table index i{full_table_name} creation is completed.")

                        self.input_tableinfo(full_table_name, cursor, conn)

        return schema_object

    def put_kube_data_to_ontune(self, info, kube_data, schema_obj):
        # Put data
        with self.db.get_resource_rdb() as (cursor, cur_dict, conn):
            # Kube Data variables
            namespace_list = kube_data["ns"]     
            node_list = kube_data["node"]
            pod_list = kube_data["pod"]
            svc_list = kube_data["svc"]
            deploy_list = kube_data["deploy"]
            sts_list = kube_data["sts"]
            ds_list = kube_data["ds"]
            rs_list = kube_data["rs"]

            # Pre-define data (여기에서의 Key는 Primary와 같은 Key값이 아니라 dictionary의 key-value의 key를 뜻함)
            manager_id = 0
            cluster_id = 0
            ref_container_list = list()
            ref_container_query = list()

            namespace_query_dict = dict()       # Key: UID
            svc_query_dict = dict()             # Key: UID
            deploy_query_dict = dict()          # Key: UID
            ds_query_dict = dict()              # Key: UID
            rs_query_dict = dict()              # Key: UID
            sts_query_dict = dict()             # Key: UID
            node_query_dict = dict()            # Key: Nodename
            node_sysco_query_dict = dict()      # Key: Nodeid
            pod_query_dict = dict()             # Key: UID
            pod_container_query_dict = dict()   # Key: Podid
            pod_device_query_dict = dict()      # Key: Devicetype

            # Check Managerinfo table
            try:
                cursor.execute(stmt.SELECT_MANAGERINFO_IP.format(info["manager_ip"]))
                result = cursor.fetchone()
                manager_id = result[0]
            except:            
                column_data = self.insert_columns_ref(schema_obj, "kubemanagerinfo")
                value_data = self.insert_values([info["manager_name"], info["manager_name"], info["manager_ip"]])
                cursor.execute(stmt.INSERT_TABLE.format("kubemanagerinfo", column_data, value_data))
                conn.commit()
                self.input_tableinfo("kubemanagerinfo", cursor, conn)
                #self.log.write("PUT", f"Kubemanagerinfo insertion is completed - {info['manager_ip']}")

                cursor.execute(stmt.SELECT_MANAGERINFO_IP.format(info["manager_ip"]))
                result = cursor.fetchone()
                manager_id = result[0]

            if not manager_id:
                #self.log.write("GET", "Kubemanagerinfo has an error. Put data process is stopped.")
                return False

            # Check Clusterinfo table
            try:
                cursor.execute(stmt.SELECT_CLUSTERINFO_IP_MGRID.format(info["cluster_address"], manager_id))
                result = cursor.fetchone()
                cluster_id = result[0]
            except:
                column_data = self.insert_columns_ref(schema_obj, "kubeclusterinfo")
                value_data = self.insert_values([manager_id, info["cluster_name"], info["cluster_name"], info["cluster_address"]])
                cursor.execute(stmt.INSERT_TABLE.format("kubeclusterinfo", column_data, value_data))
                conn.commit()
                self.input_tableinfo("kubeclusterinfo", cursor, conn)
                #self.log.write("PUT", f"Kubeclusterinfo insertion is completed - {info['cluster_address']}")

                cursor.execute(stmt.SELECT_CLUSTERINFO_IP_MGRID.format(info["cluster_address"], manager_id))
                result = cursor.fetchone()
                cluster_id = result[0]

            if not cluster_id:
                #self.log.write("GET", "Kubeclusterinfo has an error. Put data process is stopped.")
                return False

            # Check Namespace(NS) table
            try:       
                cur_dict.execute(stmt.SELECT_NAMESPACEINFO_CLUSTERID.format(cluster_id))
                namespace_query_dict = dict({x["_nsname"]:x for x in cur_dict.fetchall()})
            except:
                pass
                
            try:
                new_namespace_list = list(filter(lambda x: x['name'] not in namespace_query_dict.keys(), namespace_list))
                old_namespace_list = dict(filter(lambda x: x[0] not in list(x['name'] for x in namespace_list), namespace_query_dict.items()))
                old_ns_id_list = list(str(x[1]["_nsid"]) for x in old_namespace_list.items())
                
                # New Namespace Insertion
                for new_ns in new_namespace_list:
                    column_data = self.insert_columns_ref(schema_obj, "kubensinfo")
                    value_data = self.insert_values([cluster_id, new_ns['name'], new_ns['status'], 1])
                    cursor.execute(stmt.INSERT_TABLE.format("kubensinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubensinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubensinfo insertion is completed - {new_ns}")

                # Old Namespace Update
                if len(old_ns_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubensinfo", "_nsid", ",".join(old_ns_id_list)))
                    conn.commit()
                    self.input_tableinfo("kubensinfo", cursor, conn)

                # New NS ID Update
                try:
                    cur_dict.execute(stmt.SELECT_NAMESPACEINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    namespace_query_dict = dict({x["_nsname"]:x for x in result})
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubenamespaceinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check Nodeinfo Table
            try:
                cur_dict.execute(stmt.SELECT_NODEINFO_CLUSTERID.format(cluster_id))
                node_query_dict = dict({x["_nodename"]:x for x in cur_dict.fetchall()})
            except:
                pass

            # Namespace, Nodesysco, Pod 등의 정보는 과거 정보는 enabled=0으로 갱신, 신규 정보는 insert하도록하나,
            # Node 정보는 추가로 기존 정보의 변경사항에 대해서 Update하는 부분이 추가되므로 프로세스도 달라짐
            try:
                for node in node_list:
                    if node in node_query_dict:
                        if node_list[node]["uid"] != node_query_dict[node]["_nodeuid"]:
                            update_data = self.update_values(schema_obj, "kubenodeinfo", node_list[node])
                            cursor.execute(stmt.UPDATE_TABLE.format("kubenodeinfo", update_data, "_nodeid", node_query_dict[node]["_nodeid"]))
                            conn.commit()
                            self.input_tableinfo("kubenodeinfo", cursor, conn)
                            #self.log.write("PUT", f"Kubenodeinfo information is updated - {node}")

                    else:
                        column_data = self.insert_columns_ref(schema_obj, "kubenodeinfo")
                        value_data = self.insert_values([manager_id, cluster_id] + list(node_list[node].values()))
                        cursor.execute(stmt.INSERT_TABLE.format("kubenodeinfo", column_data, value_data))
                        conn.commit()
                        self.input_tableinfo("kubenodeinfo", cursor, conn)
                        #self.log.write("PUT", f"Kubenodeinfo insertion is completed - {node}")

                old_node_list = dict(filter(lambda x: x[0] not in node_list, node_query_dict.items()))
                old_node_id_list = list(str(x[1]["_nodeid"]) for x in old_node_list.items())

                # Old Node Update
                if len(old_node_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubenodeinfo", "_nodeid", ",".join(old_node_id_list)))
                    conn.commit()
                    self.input_tableinfo("kubenodeinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubenodeinfo enabled state is updated - {','.join(old_node_id_list)}")

                # New Node ID Update
                try:
                    cur_dict.execute(stmt.SELECT_NODEINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    node_query_dict = dict({x["_nodename"]:x for x in result})
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubenodeinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            if not node_query_dict:
                self.log.write("GET", "Kubenodeinfo is empty. Put data process is stopped.")
                return True         # Not False return

            # Check Node Systemcontainer info Table
            try:
                syscontainer_info_dict = dict({x[0]:list(y["name"] for y in x[1]["systemContainers"]) for x in kube_data["node_metric"].items()})
                syscontainer_info = list()
                sc_query_list = list()

                for node in syscontainer_info_dict:
                    syscontainer_info.extend(list({"nodename":node, "containername": x} for x in syscontainer_info_dict[node]))

                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_NODE_SYSCONTAINER_NODEID.format(nodeid_data))
                    sc_query_list = list(dict(x) for x in cur_dict.fetchall())
                except:
                    pass

                new_node_sysco_list = list(filter(lambda x: [x["nodename"], x["containername"]] not in list([y["_nodename"],y["_containername"]] for y in sc_query_list), syscontainer_info))
                old_node_sysco_list = list(filter(lambda x: [x["_nodename"], x["_containername"]] not in list([y["nodename"],y["containername"]] for y in syscontainer_info), sc_query_list))

                for sysco in new_node_sysco_list:
                    nodeid = node_query_dict[sysco["nodename"]]["_nodeid"]
                    column_data = self.insert_columns_ref(schema_obj, "kubenodesyscoinfo")
                    value_data = self.insert_values([nodeid, sysco["containername"], 1])
                    cursor.execute(stmt.INSERT_TABLE.format("kubenodesyscoinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubenodesyscoinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubenodesyscoinfo insertion is completed - {sysco['nodename']} / {sysco['containername']}")

                if old_node_sysco_list:
                    old_node_sysco_id_list = list(str(x["_syscontainerid"]) for x in old_node_sysco_list)

                    if len(old_node_sysco_id_list) > 0:
                        cursor.execute(stmt.UPDATE_ENABLED.format("kubenodesyscoinfo","_syscontainerid",",".join(old_node_sysco_id_list)))
                        conn.commit()
                        self.input_tableinfo("kubenodesyscoinfo", cursor, conn)
                        #self.log.write("PUT", f"Kubenodesyscoinfo enabled state is updated - {','.join(old_node_id_list)}")

                # New Node System Container info Update
                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_NODE_SYSCONTAINER_NODEID.format(nodeid_data))
                    result = cur_dict.fetchall()

                    for row in result:
                        nodeid = row["_nodeid"]
                        if nodeid not in node_sysco_query_dict:
                            node_sysco_query_dict[nodeid] = list()

                        node_sysco_query_dict[nodeid].append(row)
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubenodesystemcontainer info has an error. Put data process is stopped. - {str(e)}")
                return False

            # Assign Reference Containers table
            # Container 정보는 Pod 뿐만 아니라, Deploy, DS, RS 모두 포함되므로 각각의 Container 정보를 취합해서 입력해야 함
            # 하단 For Loop에서는 데이터 변수를 재선언 후(data=dict(pod) 등) pop 처리를 하나 여기에서는 직접 처리하며, 
            # 그 이유는 이후에 재사용 시 'containers' 인스턴스를 제거해주기 위함
            # 실제 처리 부분은 Deploy, DS, RS, Pod 데이터 입력 완료 후 처리 예정
            pod_container_set = set()
            for pod in pod_list:
                pod_containers = pod_list[pod].pop("containers")
                pod_container_set.update(pod_containers)
            ref_container_list.extend(list(pod_container_set))

            deploy_container_set = set()
            for deploy in deploy_list:
                deploy_containers = deploy_list[deploy].pop("containers")
                deploy_container_set.update(deploy_containers)
            ref_container_list.extend(list(deploy_container_set))

            sts_container_set = set()
            for sts in sts_list:
                sts_containers = sts_list[sts].pop("containers")
                sts_container_set.update(sts_containers)
            ref_container_list.extend(list(sts_container_set))

            ds_container_set = set()
            for ds in ds_list:
                ds_containers = ds_list[ds].pop("containers")
                ds_container_set.update(ds_containers)
            ref_container_list.extend(list(ds_container_set))

            rs_container_set = set()
            for rs in rs_list:
                rs_containers = rs_list[rs].pop("containers")
                rs_container_set.update(rs_containers)
            ref_container_list.extend(list(rs_container_set))
            
            # Check Service info table
            try:
                cur_dict.execute(stmt.SELECT_SVCINFO_CLUSTERID.format(cluster_id))
                svc_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
            except:
                pass

            try:
                old_svc_list = dict(filter(lambda x: x[0] not in svc_list, svc_query_dict.items()))
                new_svc_list = dict(filter(lambda x: x[0] not in svc_query_dict, svc_list.items()))
                old_svc_id_list = list(str(x[1]["_svcid"]) for x in old_svc_list.items())

                # New SVC Insertion
                for new_svc in new_svc_list:
                    svc_data = dict(new_svc_list[new_svc])
                    svc_ns_name = svc_data.pop("nsname")
                    svc_data["nsid"] = namespace_query_dict[svc_ns_name]["_nsid"]

                    column_data = self.insert_columns_ref(schema_obj, "kubesvcinfo")
                    value_data = self.insert_values(list(svc_data.values()))
                    cursor.execute(stmt.INSERT_TABLE.format("kubesvcinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubesvcinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubesvcinfo insertion is completed - {new_svc}")

                # Old SVC Update
                if len(old_svc_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubesvcinfo", "_svcid", ",".join(old_svc_list)))
                    conn.commit()
                    self.input_tableinfo("kubesvcinfo", cursor, conn)

                # New SVC ID Update
                try:
                    cur_dict.execute(stmt.SELECT_SVCINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    svc_query_dict = dict({x["_uid"]:x for x in result})
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubesvcinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check Deployment info table
            try:
                cur_dict.execute(stmt.SELECT_DEPLOYINFO_CLUSTERID.format(cluster_id))
                deploy_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
            except:
                pass

            try:
                old_deploy_list = dict(filter(lambda x: x[0] not in deploy_list, deploy_query_dict.items()))
                new_deploy_list = dict(filter(lambda x: x[0] not in deploy_query_dict, deploy_list.items()))
                old_deploy_id_list = list(str(x[1]["_deployid"]) for x in old_deploy_list.items())

                # New deploy Insertion
                for new_deploy in new_deploy_list:
                    deploy_data = dict(new_deploy_list[new_deploy])
                    deploy_ns_name = deploy_data.pop("nsname")
                    deploy_data["nsid"] = namespace_query_dict[deploy_ns_name]["_nsid"]

                    column_data = self.insert_columns_ref(schema_obj, "kubedeployinfo")
                    value_data = self.insert_values(list(deploy_data.values()))
                    cursor.execute(stmt.INSERT_TABLE.format("kubedeployinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubedeployinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubedeployinfo insertion is completed - {new_deploy}")

                # Old deploy Update
                if len(old_deploy_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubedeployinfo", "_deployid", ",".join(old_deploy_list)))
                    conn.commit()
                    self.input_tableinfo("kubedeployinfo", cursor, conn)

                # New deploy ID Update
                try:
                    cur_dict.execute(stmt.SELECT_DEPLOYINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    deploy_query_dict = dict({x["_uid"]:x for x in result})
                except:
                    pass            
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubedeployinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check StatefulSet info table
            try:
                cur_dict.execute(stmt.SELECT_STSINFO_CLUSTERID.format(cluster_id))
                sts_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
                old_sts_id_list = list(str(x[1]["_stsid"]) for x in old_sts_list.items())
            except:
                pass

            try:
                old_sts_list = dict(filter(lambda x: x[0] not in sts_list, sts_query_dict.items()))
                new_sts_list = dict(filter(lambda x: x[0] not in sts_query_dict, sts_list.items()))

                # New sts Insertion
                for new_sts in new_sts_list:
                    sts_data = dict(new_sts_list[new_sts])
                    sts_ns_name = sts_data.pop("nsname")
                    sts_data["nsid"] = namespace_query_dict[sts_ns_name]["_nsid"]

                    column_data = self.insert_columns_ref(schema_obj, "kubestsinfo")
                    value_data = self.insert_values(list(sts_data.values()))
                    cursor.execute(stmt.INSERT_TABLE.format("kubestsinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubestsinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubestsinfo insertion is completed - {new_sts}")

                # Old sts Update
                if len(old_sts_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubestsinfo", "_stsid", ",".join(old_sts_list)))
                    conn.commit()
                    self.input_tableinfo("kubestsinfo", cursor, conn)

                # New sts ID Update
                try:
                    cur_dict.execute(stmt.SELECT_STSINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    sts_query_dict = dict({x["_uid"]:x for x in result})
                except:
                    pass            
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubestsinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check Daemonset info table
            try:
                cur_dict.execute(stmt.SELECT_DSINFO_CLUSTERID.format(cluster_id))
                ds_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
            except:
                pass

            try:
                old_ds_list = dict(filter(lambda x: x[0] not in ds_list, ds_query_dict.items()))
                new_ds_list = dict(filter(lambda x: x[0] not in ds_query_dict, ds_list.items()))
                old_ds_id_list = list(str(x[1]["_dsid"]) for x in old_ds_list.items())

                # New ds Insertion
                for new_ds in new_ds_list:
                    ds_data = dict(new_ds_list[new_ds])
                    ds_ns_name = ds_data.pop("nsname")
                    ds_data["nsid"] = namespace_query_dict[ds_ns_name]["_nsid"]

                    column_data = self.insert_columns_ref(schema_obj, "kubedsinfo")
                    value_data = self.insert_values(list(ds_data.values()))
                    cursor.execute(stmt.INSERT_TABLE.format("kubedsinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubedsinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubedsinfo insertion is completed - {new_ds}")

                # Old ds Update
                if len(old_ds_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubedsinfo", "_dsid", ",".join(old_ds_list)))
                    conn.commit()
                    self.input_tableinfo("kubedsinfo", cursor, conn)

                # New ds ID Update
                try:
                    cur_dict.execute(stmt.SELECT_DSINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    ds_query_dict = dict({x["_uid"]:x for x in result})
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubedsinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check Replicaset info table
            try:
                cur_dict.execute(stmt.SELECT_RSINFO_CLUSTERID.format(cluster_id))
                rs_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
            except:
                pass

            try:
                old_rs_list = dict(filter(lambda x: x[0] not in rs_list, rs_query_dict.items()))
                new_rs_list = dict(filter(lambda x: x[0] not in rs_query_dict, rs_list.items()))
                old_rs_id_list = list(str(x[1]["_rsid"]) for x in old_rs_list.items())

                # New rs Insertion
                for new_rs in new_rs_list:
                    rs_data = dict(new_rs_list[new_rs])
                    rs_ns_name = rs_data.pop("nsname")
                    rs_ref_uid = rs_data.pop("refuid")
                    rs_data["nsid"] = namespace_query_dict[rs_ns_name]["_nsid"]

                    if rs_data["refkind"] == "Deployment":
                        rs_data["refid"] = deploy_query_dict[rs_ref_uid]["_deployid"]
                    elif rs_data["rerfid"] == "StatefulSet":
                        rs_data["refid"] = sts_query_dict[rs_ref_uid]["_stsid"]

                    column_data = self.insert_columns_ref(schema_obj, "kubersinfo")
                    value_data = self.insert_values(list(rs_data.values()))
                    cursor.execute(stmt.INSERT_TABLE.format("kubersinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubersinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubersinfo insertion is completed - {new_rs}")

                # Old rs Update
                if len(old_rs_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubersinfo", "_rsid", ",".join(old_rs_list)))
                    conn.commit()
                    self.input_tableinfo("kubersinfo", cursor, conn)

                # New rs ID Update
                try:
                    cur_dict.execute(stmt.SELECT_RSINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    rs_query_dict = dict({x["_uid"]:x for x in result})
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubersinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check Podinfo table
            try:
                cur_dict.execute(stmt.SELECT_PODINFO_CLUSTERID.format(cluster_id))
                pod_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
            except:
                pass

            try:
                old_pod_list = dict(filter(lambda x: x[0] not in pod_list, pod_query_dict.items()))
                new_pod_list = dict(filter(lambda x: x[0] not in pod_query_dict, pod_list.items()))
                old_pod_id_list = list(str(x[1]["_podid"]) for x in old_pod_list.items())

                # New pod Insertion
                for new_pod in new_pod_list:
                    pod_data = dict(new_pod_list[new_pod])
                    pod_node_name = pod_data.pop("nodename")
                    pod_ns_name = pod_data.pop("nsname")
                    pod_ref_uid = pod_data.pop("refuid")
                    pod_data["nsid"] = namespace_query_dict[pod_ns_name]["_nsid"]
                    pod_data["nodeid"] = node_query_dict[pod_node_name]["_nodeid"]

                    if pod_data["refkind"] == "DaemonSet":
                        pod_data["refid"] = ds_query_dict[pod_ref_uid]["_dsid"]
                    elif pod_data["rerfid"] == "ReplicaSet":
                        pod_data["refid"] = rs_query_dict[pod_ref_uid]["_rsid"]


                    column_data = self.insert_columns_ref(schema_obj, "kubepodinfo")
                    value_data = self.insert_values(list(pod_data.values()))
                    cursor.execute(stmt.INSERT_TABLE.format("kubepodinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubepodinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubepodinfo insertion is completed - {new_pod}")

                # Old pod Update
                if len(old_pod_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubepodinfo", "_podid", ",".join(old_pod_list)))
                    conn.commit()
                    self.input_tableinfo("kubepodinfo", cursor, conn)

                # New pod ID Update
                try:
                    cur_dict.execute(stmt.SELECT_PODINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    pod_query_dict = dict({x["_uid"]:x for x in result})
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubepodinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            if not pod_query_dict:
                self.log.write("GET", "Kubepodinfo is empty. Put data process is stopped.")
                return True         # Not False return

            # Check Reference Containers table
            try:
                cur_dict.execute(stmt.SELECT_REF_CONTAINERINFO_CLUSTERID.format(cluster_id))
                ref_container_query = cur_dict.fetchall()
            except:
                pass

            try:
                def rc_exist_condition_new(rc):
                    return (
                        rc["kind"] in list(x["_refobjkind"] for x in ref_container_query)
                        and rc["name"] in list(x["_refcontainername"] for x in ref_container_query)
                        and rc["image"] in list(x["_image"] for x in ref_container_query)
                    )

                def rc_exist_condition_old(rc):
                    return (
                        rc["_refobjkind"] in list(x["kind"] for x in ref_container_list)
                        and rc["_refcontainername"] in list(x["name"] for x in ref_container_list)
                        and rc["_image"] in list(x["image"] for x in ref_container_list)
                    )

                old_rc_list = dict(filter(lambda x: rc_exist_condition_old(x), ref_container_query))
                new_rc_list = dict(filter(lambda x: rc_exist_condition_new(x), ref_container_list))
                old_pod_id_list = list(str(x["_refcontainerid"]) for x in old_rc_list)

                # New rc Insertion
                for new_rc in new_rc_list:
                    rc_data = {
                        "objkind": new_rc["kind"],
                        "objid": 0,
                        "clusterid": cluster_id,
                        "name": new_rc["name"],
                        "image": new_rc["image"],
                        "ports": new_rc["ports"],
                        "env": new_rc["env"],
                        "resources": new_rc["resources"],
                        "volumemounts": new_rc["volumemounts"]
                    }
                    
                    column_data = self.insert_columns_ref(schema_obj, "kuberefcontainerinfo")
                    value_data = self.insert_values(list(svc_data.values()))
                    cursor.execute(stmt.INSERT_TABLE.format("kuberefcontainerinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kuberefcontainerinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubesvcinfo insertion is completed - {new_svc}")

                # Old rc Update
                if len(old_pod_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kuberefcontainerinfo", "_refcontainerid", ",".join(old_svc_list)))
                    conn.commit()
                    self.input_tableinfo("kuberefcontainerinfo", cursor, conn)

                # New SVC ID Update
                try:
                    cur_dict.execute(stmt.SELECT_REF_CONTAINERINFO_CLUSTERID.format(cluster_id))
                    ref_container_query = cur_dict.fetchall()
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"kuberefcontainerinfo has an error. Put data process is stopped. - {str(e)}")
                return False


            # Check Container table
            try:
                container_info = list()
                container_query_list = list()
                
                containerinfo_dict = dict({x[0]:dict({
                    y["podRef"]["uid"]:y["containers"] for y in x[1]
                }) for x in kube_data["pod_metric"].items()})

                for node in containerinfo_dict:
                    for pod in containerinfo_dict[node]:
                        container_info.extend(list({
                            "name": x["name"],
                            "starttime": x["startTime"],
                            "pod": pod,
                            "node": node
                        } for x in containerinfo_dict[node][pod]))

                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_CONTAINERINFO_NODEID.format(nodeid_data))
                    container_query_list = list(dict(x) for x in cur_dict.fetchall())
                except:
                    pass

                # Container 정보는 추가만 되며, 삭제나 enabled를 별도로 설정하지 않음
                new_container_list = list(filter(lambda x: [x["node"],x["pod"],x["name"]] not in list([y["_nodename"],y["_poduid"],y["_containername"]] for y in container_query_list), container_info))

                for container in new_container_list:
                    podid = pod_query_dict[container["pod"]]["_podid"]
                    column_data = self.insert_columns_ref(schema_obj, "kubecontainerinfo")
                    value_data = self.insert_values([podid, container["name"], self.svtime_to_timestampz(container["starttime"])])
                    cursor.execute(stmt.INSERT_TABLE.format("kubecontainerinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubecontainerinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubecontainerinfo insertion is completed - {podid} / {container['name']}")

                # New Pod Container info Update
                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_CONTAINERINFO_NODEID.format(nodeid_data))
                    result = cur_dict.fetchall()

                    for row in result:
                        podid = row["_podid"]
                        if podid not in pod_container_query_dict:
                            pod_container_query_dict[podid] = list()

                        pod_container_query_dict[podid].append(row)
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubecontainerinfo has an error. Put data process is stopped. - {str(e)}")
                return False
                
            # Check Pod Device(Network, Filesystem) info table
            try:
                device_type_set = {'network','volume'}
                device_info = dict()
                deviceinfo_query_list = dict()

                for device_type in device_type_set:
                    device_info[device_type] = list()

                    try:
                        cursor.execute(stmt.SELECT_PODDEVICEINFO_DEVICETYPE.format(device_type))
                        deviceinfo_query_list[device_type] = cursor.fetchall()
                    except:
                        deviceinfo_query_list[device_type] = list()

                netdeviceinfo_dict = dict({x[0]:dict({
                    y["podRef"]["uid"]:list(
                        z["name"] for z in y["network"]["interfaces"]
                    ) for y in x[1] if "network" in y
                }) for x in kube_data["pod_metric"].items()})

                for node in netdeviceinfo_dict:
                    for pod in netdeviceinfo_dict[node]:
                        device_info['network'].extend(netdeviceinfo_dict[node][pod])

                voldeviceinfo_dict = dict({x[0]:dict({
                    y["podRef"]["uid"]:list(
                        z["name"] for z in y["volume"]
                    ) for y in x[1] if "volume" in y
                }) for x in kube_data["pod_metric"].items()})

                for node in voldeviceinfo_dict:
                    for pod in voldeviceinfo_dict[node]:
                        device_info['volume'].extend(voldeviceinfo_dict[node][pod])

                device_info_set = dict({x[0]:set(x[1]) for x in device_info.items()})
                new_dev_info_set = dict({x:set(filter(lambda y: y not in list(z[1] for z in deviceinfo_query_list[x]), device_info_set[x])) for x in device_type_set})

                for devtype in device_type_set:
                    for devinfo in new_dev_info_set[devtype]:
                        column_data = self.insert_columns_ref(schema_obj, "kubepoddeviceinfo")
                        value_data = self.insert_values([devinfo, devtype])
                        cursor.execute(stmt.INSERT_TABLE.format("kubepoddeviceinfo", column_data, value_data))
                        conn.commit()
                        self.input_tableinfo("kubepoddeviceinfo", cursor, conn)
                        #self.log.write("PUT", f"Kubepoddeviceinfo insertion is completed - {devtype} / {devinfo}")

                    # New Pod Device info Update
                    try:
                        cur_dict.execute(stmt.SELECT_PODDEVICEINFO_DEVICETYPE.format(devtype))
                        pod_device_query_dict[devtype] = cur_dict.fetchall()
                    except:
                        pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubedeviceinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Update Lastrealtimeperf table
            try:
                for node in node_query_dict:
                    node_data = kube_data["node_metric"][node]

                    network_prev_cum_usage = self.system_var.get_network_metric("lastrealtimeperf", node)                
                    network_cum_usage = sum(list(x["rxBytes"]+x["txBytes"] for x in node_data["network"]["interfaces"]))
                    network_usage = network_cum_usage - network_prev_cum_usage if network_prev_cum_usage else 0
                    ontunetime = self.get_ontunetime(cursor)

                    node_perf = [
                        node_query_dict[node]["_nodeid"],
                        ontunetime,
                        self.calculate('cpu_usage_percent', [node_data["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]),
                        self.calculate('memory_used_percent', node_data["memory"]),
                        self.calculate('memory_swap_percent', node_data["memory"]),
                        self.calculate('memory_size', node_data["memory"]),
                        node_data["memory"]["rssBytes"],
                        self.calculate('network', [network_usage]),
                        self.calculate('fs_usage_percent', node_data["fs"]),
                        self.calculate('fs_total_size', node_data["fs"]),
                        self.calculate('fs_inode_usage_percent', node_data["fs"]),
                        self.calculate('fs_usage_percent', node_data["runtime"]["imageFs"]),
                        node_data["rlimit"]["curproc"]
                    ]

                    self.system_var.set_network_metric("lastrealtimeperf", node, network_cum_usage)

                    column_data = self.insert_columns_metric(schema_obj, "kubelastrealtimeperf")
                    value_data = self.insert_values(node_perf)

                    cursor.execute(stmt.DELETE_LASTREALTIMEPERF.format(node_query_dict[node]["_nodeid"]))
                    cursor.execute(stmt.INSERT_TABLE.format("kubelastrealtimeperf", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubelastrealtimeperf", cursor, conn, ontunetime)
                    #self.log.write("PUT", f"Kubelastrealtimeperf update is completed - {node_query_dict[node]['_nodeid']}")
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubelastrealtimeperf has an error. Put data process is stopped. - {str(e)}")
                return False

            # Update Realtime table
            ontunetime = self.get_ontunetime(cursor)
            agenttime = self.get_agenttime(cursor)

            try:
                table_postfix = f"_{datetime.now().strftime('%y%m%d')}00"

                for node in node_query_dict:
                    # Setting node data
                    node_data = kube_data["node_metric"][node]
                    nodeid = node_query_dict[node]["_nodeid"]

                    network_prev_cum_usage = self.system_var.get_network_metric("nodeperf", node)
                    network_cum_usage = [
                        self.calculate('network', [sum(list(x["rxBytes"]+x["txBytes"] for x in node_data["network"]["interfaces"]))]),
                        self.calculate('network', [sum(list(x["rxBytes"] for x in node_data["network"]["interfaces"]))]),
                        self.calculate('network', [sum(list(x["txBytes"] for x in node_data["network"]["interfaces"]))]),
                        sum(list(x["rxErrors"] for x in node_data["network"]["interfaces"])),
                        sum(list(x["txErrors"] for x in node_data["network"]["interfaces"]))
                    ]
                    network_usage = list(network_cum_usage[x] - network_prev_cum_usage[x] if network_prev_cum_usage else 0 for x in range(len(network_cum_usage)))

                    # Insert nodeperf metric data
                    realtime_nodeperf = [
                        nodeid,
                        ontunetime,
                        agenttime,
                        self.calculate('cpu_usage_percent', [node_data["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]),
                        self.calculate('memory_used_percent', node_data["memory"]),
                        self.calculate('memory_swap_percent', node_data["memory"]),
                        self.calculate('memory_size', node_data["memory"]),
                        node_data["memory"]["rssBytes"]
                    ] + network_usage + [
                        self.calculate('fs_usage_percent', node_data["fs"]),
                        self.calculate('fs_total_size', node_data["fs"]),
                        self.calculate('fs_free_size', node_data["fs"]),
                        self.calculate('fs_inode_usage_percent', node_data["fs"]),
                        self.calculate('fs_inode_total_size', node_data["fs"]),
                        self.calculate('fs_inode_free_size', node_data["fs"]),
                        self.calculate('fs_usage_percent', node_data["runtime"]["imageFs"]),
                        node_data["rlimit"]["maxpid"],
                        node_data["rlimit"]["curproc"]
                    ]

                    self.system_var.set_network_metric("nodeperf", node, network_cum_usage)

                    table_name = f"realtimekubenodeperf{table_postfix}"
                    column_data = self.insert_columns_metric(schema_obj, "kubenodeperf")
                    value_data = self.insert_values(realtime_nodeperf)

                    cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                    conn.commit()
                    self.input_tableinfo(table_name, cursor, conn, ontunetime)
                    #self.log.write("PUT", f"{table_name} update is completed - {node_query_dict[node]['_nodeid']}")

                    # Insert node system container metric data
                    sysco_data = dict({x["name"]: {
                        "cpu": x["cpu"],
                        "memory": x["memory"]
                    } for x in node_data["systemContainers"]})

                    for sysco_query_data in node_sysco_query_dict[nodeid]:
                        containername = sysco_query_data["_containername"]

                        realtime_node_sysco = [
                            nodeid,
                            sysco_query_data["_syscontainerid"],
                            ontunetime,
                            agenttime,
                            self.calculate('cpu_usage_percent', [sysco_data[containername]["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]) if "usageNanoCores" in sysco_data[containername]["cpu"] else 0,
                            self.calculate('memory_used_percent', sysco_data[containername]["memory"]),
                            self.calculate('memory_swap_percent', sysco_data[containername]["memory"]),
                            self.calculate('memory_size', sysco_data[containername]["memory"]),
                            sysco_data[containername]["memory"]["rssBytes"] if "rssBytes" in sysco_data[containername]["memory"] else 0
                        ]

                        table_name = f"realtimekubenodesysco{table_postfix}"
                        column_data = self.insert_columns_metric(schema_obj, "kubenodesysco")
                        value_data = self.insert_values(realtime_node_sysco)

                        cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                        conn.commit()
                        self.input_tableinfo(table_name, cursor, conn, ontunetime)

                    #self.log.write("PUT", f"{table_name} update is completed - {node_query_dict[node]['_nodeid']}")

                    # Setting node data
                    pod_data = kube_data["pod_metric"][node]

                    for pod in pod_data:
                        uid = pod["podRef"]["uid"]
                        podid = pod_query_dict[uid]["_podid"]

                        network_prev_cum_usage = self.system_var.get_network_metric("podperf", podid)
                        network_cum_usage = [
                            self.calculate('network', [sum(list(x["rxBytes"]+x["txBytes"] for x in pod["network"]["interfaces"]))]) if "network" in pod and "interfaces" in pod["network"] else 0,
                            self.calculate('network', [sum(list(x["rxBytes"] for x in pod["network"]["interfaces"]))]) if "network" in pod and "interfaces" in pod["network"] else 0,
                            self.calculate('network', [sum(list(x["txBytes"] for x in pod["network"]["interfaces"]))]) if "network" in pod and "interfaces" in pod["network"] else 0,
                            sum(list(x["rxErrors"] for x in pod["network"]["interfaces"])) if "network" in pod and "interfaces" in pod["network"] else 0,
                            sum(list(x["txErrors"] for x in pod["network"]["interfaces"])) if "network" in pod and "interfaces" in pod["network"] else 0,
                        ]
                        network_usage = list(network_cum_usage[x] - network_prev_cum_usage[x] if network_prev_cum_usage else 0 for x in range(len(network_cum_usage)))

                        # Insert pod metric data
                        realtime_podperf = [
                            podid,
                            ontunetime,
                            agenttime,
                            self.calculate('cpu_usage_percent', [pod["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]) if "cpu" in pod and "usageNanoCores" in pod["cpu"] else 0,
                            self.calculate('memory_used_percent', pod["memory"]),
                            self.calculate('memory_swap_percent', pod["memory"]),
                            self.calculate('memory_size', pod["memory"]),
                            pod["memory"]["rssBytes"] if "memory" in pod and "rssBytes" in pod["memory"] else 0
                        ] + network_usage + [
                            sum(int(x["usedBytes"]) for x in pod["volume"]) if "volume" in pod and "usedBytes" in pod["volume"][0] else 0,
                            sum(int(x["inodesUsed"]) for x in pod["volume"]) if "volume" in pod and "inodesUsed" in pod["volume"][0] else 0,
                            pod["ephemeral-storage"]["usedBytes"],
                            pod["ephemeral-storage"]["inodesUsed"],
                            pod["process_stats"]["process_count"] if "ephemeral-storage" in pod and "process_count" in pod["process_stats"] else 0
                        ]

                        self.system_var.set_network_metric("podperf", podid, network_cum_usage)

                        table_name = f"realtimekubepodperf{table_postfix}"
                        column_data = self.insert_columns_metric(schema_obj, "kubepodperf")
                        value_data = self.insert_values(realtime_podperf)

                        cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                        conn.commit()
                        self.input_tableinfo(table_name, cursor, conn, ontunetime)
                        #self.log.write("PUT", f"{table_name} update is completed - {uid}")

                        # Insert pod container metric data
                        for pod_container in pod["containers"]:
                            realtime_containerperf = [
                                list(filter(lambda x: x["_containername"] == pod_container["name"], list(pod_container_query_dict[podid])))[0]["_containerid"],
                                ontunetime,
                                agenttime,
                                self.calculate('cpu_usage_percent', [pod_container["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]) if "cpu" in pod_container and "usageNanoCores" in pod_container["cpu"] else 0,
                                self.calculate('memory_used_percent', pod_container["memory"]),
                                self.calculate('memory_swap_percent', pod_container["memory"]),
                                self.calculate('memory_size', pod_container["memory"]),
                                pod_container["memory"]["rssBytes"] if "memory" in pod_container and "rssBytes" in pod_container["memory"] else 0,
                                pod_container["rootfs"]["usedBytes"],
                                pod_container["rootfs"]["inodesUsed"],
                                pod_container["logs"]["usedBytes"],
                                pod_container["logs"]["inodesUsed"]
                            ]

                            table_name = f"realtimekubecontainerperf{table_postfix}"
                            column_data = self.insert_columns_metric(schema_obj, "kubecontainerperf")
                            value_data = self.insert_values(realtime_containerperf)

                            cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                            conn.commit()
                            self.input_tableinfo(table_name, cursor, conn, ontunetime)

                        #self.log.write("PUT", f"{table_name} update is completed - {uid}")

                        # Insert pod device metric data
                        if "network" in pod and "interfaces" in pod["network"]:
                            for pod_network in pod["network"]["interfaces"]:
                                deviceid = list(filter(lambda x: x["_devicename"] == pod_network["name"], list(pod_device_query_dict["network"])))[0]["_deviceid"]
                                podnet_key = f"{podid}_{deviceid}"

                                network_prev_cum_usage = self.system_var.get_network_metric("podnet", podnet_key)
                                network_cum_usage = [
                                    self.calculate('network', [pod_network["rxBytes"] + pod_network["txBytes"]]),
                                    self.calculate('network', [pod_network["rxBytes"]]),
                                    self.calculate('network', [pod_network["txBytes"]]),
                                    pod_network["rxErrors"],
                                    pod_network["txErrors"]                                                        
                                ]
                                network_usage = list(network_cum_usage[x] - network_prev_cum_usage[x] if network_prev_cum_usage else 0 for x in range(len(network_cum_usage)))

                                realtime_podnet = [
                                    podid,
                                    deviceid,
                                    ontunetime,
                                    agenttime
                                ] + network_usage

                                self.system_var.set_network_metric("podnet", podnet_key, network_cum_usage)

                                table_name = f"realtimekubepodnet{table_postfix}"
                                column_data = self.insert_columns_metric(schema_obj, "kubepodnet")
                                value_data = self.insert_values(realtime_podnet)

                                cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                                conn.commit()
                                self.input_tableinfo(table_name, cursor, conn, ontunetime)

                        if "volume" in pod:
                            for pod_volume in pod["volume"]:
                                realtime_podvol = [
                                    podid,
                                    list(filter(lambda x: x["_devicename"] == pod_volume["name"], list(pod_device_query_dict["volume"])))[0]["_deviceid"],
                                    ontunetime,
                                    agenttime,
                                    pod_volume["usedBytes"],
                                    pod_volume["inodesUsed"]
                                ]

                                table_name = f"realtimekubepodvol{table_postfix}"
                                column_data = self.insert_columns_metric(schema_obj, "kubepodvol")
                                value_data = self.insert_values(realtime_podvol)

                                cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                                conn.commit()
                                self.input_tableinfo(table_name, cursor, conn, ontunetime)

                        #self.log.write("PUT", f"{table_name} update is completed - {uid}")

            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kube realtime tables have an error. Put data process is stopped. - {str(e)}")
                return False

            # Update Average table 
            def insert_average_table(table_midfix, key_columns):
                try:
                    today_postfix = f"_{date.today().strftime('%y%m%d')}00"
                    prev_postfix = f"_{(date.today() - timedelta(1)).strftime('%y%m%d')}00"
                    lt_prev_ontunetime = ontunetime - KUBE_MGR_LT_INTERVAL

                    from_clause = str()
                    table_st_prev_name = f"realtime{table_midfix}{prev_postfix}"
                    table_st_name = f"realtime{table_midfix}{today_postfix}"
                    table_lt_name = f"avg{table_midfix}{today_postfix}"

                    # Yesterday table check
                    cursor.execute(stmt.SELECT_PG_TABLES_TABLENAME_COUNT_MET.format(table_midfix, prev_postfix))
                    result = cursor.fetchone()
                    if result[0] > 0:
                        from_clause = f"(select * from {table_st_prev_name} union all select * from {table_st_name}) t"
                    else:
                        from_clause = table_st_name

                    # Between으로 하지 않는 이유는 lt_prev_ontunetime보다 GTE가 아니라 GT가 되어야 하기 때문
                    select_clause = self.select_average_columns(schema_obj, table_midfix, key_columns)
                    where_clause = f"_ontunetime > {lt_prev_ontunetime} and _ontunetime <= {ontunetime}"
                    group_clause = f" group by {','.join(key_columns)}"
                    
                    cursor.execute(stmt.SELECT_TABLE.format(select_clause, from_clause, where_clause, group_clause))
                    result = cursor.fetchall()

                    for row in result:
                        column_data = self.insert_columns_metric(schema_obj, table_midfix)
                        value_data = self.insert_values(list(row[:len(key_columns)]) + [ontunetime, agenttime] + list(row[len(key_columns):]))
                        
                        cursor.execute(stmt.INSERT_TABLE.format(table_lt_name, column_data, value_data))
                        conn.commit()
                        self.input_tableinfo(table_name, cursor, conn, ontunetime)

                    #self.log.write("PUT", f"{table_lt_name} update is completed - {ontunetime}")
                except Exception as e:
                    conn.rollback()
                    self.log.write("GET", f"Kube average tables have an error. Put data process is stopped. - {str(e)}")

            if self.system_var.get_duration() % KUBE_MGR_LT_INTERVAL == 0:
                insert_average_table("kubenodeperf", ["_nodeid"])
                insert_average_table("kubenodesysco", ["_nodeid","_syscontainerid"])
                insert_average_table("kubepodperf", ["_podid"])
                insert_average_table("kubecontainerperf", ["_containerid"])
                insert_average_table("kubepodnet", ["_podid","_deviceid"])
                insert_average_table("kubepodvol", ["_podid","_deviceid"])
