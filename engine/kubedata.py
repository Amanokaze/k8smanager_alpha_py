import os
import json
import copy
from kubernetes import client
from engine import utils

DEFAULT_INFO = {
    "KUBE_HOST": "http://192.168.0.138:6443",
    "KUBE_API_KEY": "eyJhbGciOiJSUzI1NiIsImtpZCI6IlRyYzA4SjdEZUloZGM0M0pvODRJOTVqUngzdTB6dzE1RXBGVWQ1NV9qZEkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJhZG1pbi11c2VyLXRva2VuLXFkOGY1Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImFkbWluLXVzZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiIyYTJmYWMzZi1kOWQxLTQ3MmUtODdmMi0xYjNmZDQ5ZjQxMTEiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06YWRtaW4tdXNlciJ9.HygvQ7eaenNsMiohsR4rButEQjwecXFF_9OwrKQI_yMN1DeMuFqtRq0jk_-bvaY8kT3AXY2uekkIpXusw5C1D0KX-WDUpfVRa7OO53JEbTfJBE-Ki4md3v7aJTni_lyORJoZ45ziKbofc2z0_g87e3U2xeJtVOFZTdjniIMFCHkgl8qF2wDK5MN0WapGbnt8tj3nxdn4vlnK8fpp66GkvQ8-x3TFOdD-koqVl5JGg9Q-K72V_fCFplcyBUcaogBNUCXLpf2Ck4i-kUtTDouWKmw8klqXCbCCVotTHQk13A4EFA-0n9Gm-ihW-3ANuNP9t3F_ZJqz8FRxjEuUIlI7nw",
    "KUBE_MGR_ST_INTERVAL": 10,
    "KUBE_MGR_LT_INTERVAL": 600,
    "KUBE_CLUSTER_NAME": "kubernetes",
    "KUBE_API_VERSION": "v1",
    "KUBE_APIS_VERSION": "v1",
    "DEFAULT_ENABLED_RESOURCES": [
        'Namespace', 'Node', 'Pod', 'Service', 'Ingress', 'Deployment', 'StatefulSet', 'DaemonSet', 'ReplicaSet', 'Event', 'PersistentVolumeClaim', 'StorageClass'
    ]
}

class Kubedata:
    def __init__(self, log):
        # Initialization Variables
        self.ns_data = dict()
        self.node_data = dict()
        self.pod_data = dict()
        self.svc_data = dict()
        self.ing_data = dict()
        self.ds_data = dict()
        self.rs_data = dict()
        self.deploy_data = dict()
        self.sts_data = dict()

        self.node_metric_data = dict()
        self.pod_metric_data = dict()

        # Assign Initial Data
        self.log = log

        self.host = os.environ["KUBE_HOST"] if "KUBE_HOST" in os.environ else DEFAULT_INFO["KUBE_HOST"]
        self.api_key = os.environ["KUBE_API_KEY"] if "KUBE_API_KEY" in os.environ else DEFAULT_INFO["KUBE_API_KEY"]
        self.st_interval = int(os.environ["KUBE_MGR_ST_INTERVAL"])  if "KUBE_MGR_ST_INTERVAL" in os.environ else DEFAULT_INFO["KUBE_MGR_ST_INTERVAL"]
        self.lt_interval = int(os.environ["KUBE_MGR_LT_INTERVAL"])  if "KUBE_MGR_LT_INTERVAL" in os.environ else DEFAULT_INFO["KUBE_MGR_LT_INTERVAL"]
        self.cluster_name = os.environ["KUBE_CLUSTER_NAME"] if "KUBE_CLUSTER_NAME" in os.environ else DEFAULT_INFO["KUBE_CLUSTER_NAME"]

        self.cluster_address = str()
        self.data_exist = False
        self.resource_info = dict()

        # Kube Cluster 접속은 최초 1회만 이루어지며, Thread 별로 접속을 보장하지 않음
        self.cfg = client.Configuration()
        self.cfg.api_key['authorization'] = self.api_key
        self.cfg.api_key_prefix['authorization'] = 'Bearer'
        self.cfg.host = self.host
        self.cfg.verify_ssl = True
        self.cfg.ssl_ca_cert = 'ca.crt'

        self.get_api_version()

    def get_api_version(self):
        try:
            default_api_version = DEFAULT_INFO["KUBE_API_VERSION"]
            default_apis_version = DEFAULT_INFO["KUBE_APIS_VERSION"]

            api_version_info = client.CoreApi(client.ApiClient(self.cfg)).get_api_versions()
            self.cluster_address = api_version_info.server_address_by_client_cid_rs[0].server_address.split(":")[0]
            default_api_version = api_version_info.versions[0]

            def update_enabled(resource, kind):
                enabled_dict = {"enabled": 1 if kind in DEFAULT_INFO["DEFAULT_ENABLED_RESOURCES"] else 0}
                resource_info = copy.deepcopy(resource)
                resource_info.update(enabled_dict)
                return resource_info

            # 신 버전이 추가되면 해당 분기문의 내용이 늘어나야 할 것
            if default_api_version == "v1":
                result = client.CoreV1Api(client.ApiClient(self.cfg)).get_api_resources()
                api_resources = result.resources
                api_resource_info = {
                    "apiclass": "CoreV1Api",
                    "version": result.group_version,
                    "endpoint": "api/"+ result.group_version
                }
                self.resource_info.update(dict({x.kind:update_enabled(api_resource_info, x.kind) for x in api_resources}))

            apis_version_info = client.ApisApi(client.ApiClient(self.cfg)).get_api_versions()
            default_apis_version = apis_version_info.api_version

            def get_resource_info(apiclass, data):
                try:
                    apis_resources = data.resources
                    apis_resource_info = {
                        "apiclass": apiclass,
                        "version": data.api_version,
                        "endpoint": "apis/"+ data.group_version
                    }
                    return dict({x.kind:update_enabled(apis_resource_info, x.kind) for x in apis_resources})
                except: 
                    return dict()

            # 신 버전이 추가되면 해당 분기문의 내용이 늘어나야 할 것
            if default_apis_version == "v1":
                result = client.AppsV1Api(client.ApiClient(self.cfg)).get_api_resources()                    
                self.resource_info.update(get_resource_info("AppsV1Api", result))

                result = client.NetworkingV1Api(client.ApiClient(self.cfg)).get_api_resources()
                self.resource_info.update(get_resource_info("NetworkingV1Api", result))

                result = client.StorageV1Api(client.ApiClient(self.cfg)).get_api_resources()
                self.resource_info.update(get_resource_info("StorageV1Api", result))
        except Exception as e:
            self.log.write('ERR', str(e))

    def get_api(self, resources):
        # API 유형 및 If 분기조건은 향후 버전 별로 추가될 수 있음
        core_v1_api = client.CoreV1Api(client.ApiClient(self.cfg))
        apps_v1_api = client.AppsV1Api(client.ApiClient(self.cfg))
        network_v1_api = client.NetworkingV1Api(client.ApiClient(self.cfg))
        storage_v1_api = client.StorageV1Api(client.ApiClient(self.cfg))

        def get_apiclass(kind):
            apiclass = resources[kind]["_apiclass"]

            if apiclass == "CoreV1Api":
                return core_v1_api
            elif apiclass == "AppsV1Api":
                return apps_v1_api
            elif apiclass == "NetworkingV1Api":
                return network_v1_api
            elif apiclass == "StorageV1Api":
                return storage_v1_api
            else:
                return None
        
        def resource_enabled(kind):
            return resources[kind]["_enabled"] == 1
            
        try:
            # Namespace, Node, Pod은 Enalbed 여부와는 상관없이 무조건 동작
            self.get_kube_ns_data(get_apiclass("Namespace"))
            self.get_kube_node_data(get_apiclass("Node"))
            self.get_kube_pod_data(get_apiclass("Pod"))

            if resource_enabled("Service"):
                self.get_kube_svc_data(get_apiclass("Service"))

            if resource_enabled("Ingress"):
                self.get_kube_ing_data(get_apiclass("Ingress"))
            
            if resource_enabled("DaemonSet"):
                self.get_kube_ds_data(get_apiclass("DaemonSet"))

            if resource_enabled("ReplicaSet"):
                self.get_kube_rs_data(get_apiclass("ReplicaSet"))

            if resource_enabled("Deployment"):
                self.get_kube_deploy_data(get_apiclass("Deployment"))

            if resource_enabled("StatefulSet"):
                self.get_kube_sts_data(get_apiclass("StatefulSet"))

            if self.node_data and self.ns_data:
                self.data_exist = True
        except Exception as e:
            self.log.write('ERR', str(e))

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
                    "nodetype": node.metadata.labels["node"],
                    "enabled": 1,
                    "state": 1,
                    "connected": 1,
                    "starttime": utils.datetime_to_timestampz(node.metadata.creation_timestamp),
                    "kernelversion": node.status.node_info.kernel_version,
                    "osimage": node.status.node_info.os_image,
                    "osname": node.status.node_info.operating_system,
                    "containerruntimever": node.status.node_info.container_runtime_version,
                    "kubeletver": node.status.node_info.kubelet_version,
                    "kubeproxyver": node.status.node_info.kube_proxy_version,
                    "cpuarch": node.status.node_info.architecture,
                    "cpucount": node.status.capacity["cpu"],
                    "ephemeralstorage": utils.change_quantity_unit(node.status.capacity["ephemeral-storage"]),
                    "memorysize": utils.change_quantity_unit(node.status.capacity["memory"]),
                    "pods": node.status.capacity["pods"],
                    "ip": node.status.addresses[0].address
                }

                try:
                    node_stats = api.connect_get_node_proxy_with_path(nodename, "stats/summary")
                    node_stats_json = json.loads(node_stats.strip("'<>() ").replace("'",'"'))
                    node_metric_data[nodename] = node_stats_json['node']
                    pod_metric_data[nodename] = node_stats_json['pods']
                except client.rest.ApiException as e:
                    node_metric_data[nodename] = dict()
                    pod_metric_data[nodename] = dict()
                    node_data[nodename]["state"] = 0
                    node_data[nodename]["connected"] = 0

            self.log.write("GET", "Kube Node Data Import is completed.")

            self.node_data = node_data
            self.node_metric_data = node_metric_data
            self.pod_metric_data = pod_metric_data

        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_pod_data(self, api):
        try:
            pods = api.list_pod_for_all_namespaces()
            pod_data = dict()

            for pod in pods.items:
                restarttime = 0
                restartcount = 0
                annotation = str()

                if pod.status.container_statuses:
                    restarttime = max(list(utils.datetime_to_timestampz(x.state.running.started_at) if x.state.running else 0 for x in pod.status.container_statuses))
                    restartcount = sum(list(utils.nvl_zero(x.restart_count) for x in pod.status.container_statuses))

                conditions = ",".join(list(
                    (f"{x.type}:{x.reason}" if x.reason else f"{x.type}") +
                    (f"-{utils.datetime_to_timestampz(x.last_transition_time)}") + 
                    (f":{utils.msg_str(x.message)}" if x.message else "")
                    for x in pod.status.conditions
                )) if pod.status.conditions else f"{pod.status.reason}:{utils.msg_str(pod.status.message)}"

                if pod.metadata.annotations:
                    if "kubernetes.io/config.hash" in pod.metadata.annotations:
                        annotation = pod.metadata.annotations["kubernetes.io/config.hash"]
                    elif "kubernetes.io/config.mirror" in pod.metadata.annotations:
                        annotation = pod.metadata.annotations["kubernetes.io/config.mirror"]

                node_name = pod.spec.node_name
                node_type = self.node_data[node_name]["nodetype"]
                pod_name = pod.metadata.name

                static_pod_name = pod_name[:-len(node_name)-1] if node_type == "master" and pod_name[-len(node_name):] == node_name else str()
                
                pod_data[pod.metadata.uid] = {
                    "nodeid": 0,
                    "nsid": 0,
                    "uid": pod.metadata.uid,
                    "annotationuid": annotation,
                    "name": pod_name,
                    "starttime": utils.datetime_to_timestampz(pod.metadata.creation_timestamp),
                    "restartpolicy": pod.spec.restart_policy, 
                    "serviceaccount": pod.spec.service_account,
                    "status": pod.status.phase,
                    "hostip": utils.nvl_str(pod.status.host_ip),
                    "podip": utils.nvl_str(pod.status.pod_ip),
                    "restartcount": restartcount,
                    "restarttime": restarttime,
                    "condition": conditions,
                    "staticpod": static_pod_name,
                    "nodename": node_name,
                    "nsname": pod.metadata.namespace,
                    "refkind": pod.metadata.owner_references[0].kind if pod.metadata.owner_references else "",
                    "refid": 0,
                    "refuid": pod.metadata.owner_references[0].uid if pod.metadata.owner_references else "",
                    "label": pod.metadata.labels if pod.metadata.labels else dict(),
                    "containers": list({
                        "uid": pod.metadata.uid,
                        "name": x.name,
                        "image": x.image,
                        "ports": json.dumps(str(x.ports))[1:-1].replace("'", '"') if x.ports else "",
                        "env": json.dumps(str(x.env))[1:-1].replace("'", '"') if x.env else "",
                        "resources": json.dumps(str(x.resources))[1:-1].replace("'", '"') if x.resources else "",
                        "volumemounts": json.dumps(str(x.volume_mounts))[1:-1].replace("'", '"') if x.volume_mounts else ""
                    } for x in pod.spec.containers)
                }

            self.log.write("GET", "Kube Pod Data Import is completed.")
            self.pod_data = pod_data
        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_ns_data(self, api):
        try:
            nslist = api.list_namespace()
            self.ns_data = list({'name': x.metadata.name, 'status': x.status.phase} for x in nslist.items)

            self.log.write("GET", "Kube Namespace Data Import is completed.")
        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_svc_data(self, api):
        try:
            services = api.list_service_for_all_namespaces()
            svc_data = dict()
            for svc in services.items:
                svc_data[svc.metadata.uid] = {
                    "nsid": 0,
                    "name": svc.metadata.name,
                    "uid": svc.metadata.uid,
                    "starttime": utils.datetime_to_timestampz(svc.metadata.creation_timestamp),
                    "servicetype": svc.spec.type,
                    "clusterip": svc.spec.cluster_ip,
                    "ports": utils.dict_port_to_str(svc.spec.ports),
                    "label": svc.metadata.labels if svc.metadata.labels else dict(),
                    "selector": svc.spec.selector if svc.spec.selector else dict(),
                    "nsname": svc.metadata.namespace
                }

            self.log.write("GET", "Kube Service Data Import is completed.")

            self.svc_data = svc_data                
        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_ing_data(self, api):
        try:
            ingresses = api.list_ingress_for_all_namespaces()
            ing_data = dict()

            for ing in ingresses.items:
                ing_host_data = list()

                if ing.spec.default_backend:
                    dbe = ing.spec.default_backend

                    if dbe.service:
                        ing_host_data.append({
                            "backendtype": "service",
                            "backendname": dbe.service.name,
                            "hostname": "*",
                            "pathtype": "",
                            "path": "",
                            "svcport": dbe.service.port.number,
                            "rscapigroup": "",
                            "rsckind": "",
                            "uid": ing.metadata.uid
                        })
                    elif dbe.resrouce:
                        ing_host_data.append({
                            "backendtype": "resource",
                            "backendname": dbe.resource.name,
                            "hostname": "*",
                            "pathtype": "",
                            "path": "",
                            "svcport": "",
                            "rscapigroup": dbe.resource.api_group,
                            "rsckind": dbe.resource.kind,
                            "uid": ing.metadata.uid
                        })
                for rule in ing.spec.rules:
                    hostname = rule.host if rule.host else "*"
                    for path in rule.http.paths:
                        if path.backend.service:
                            ing_host_data.append({
                                "backendtype": "service",
                                "backendname": path.backend.service.name,
                                "hostname": hostname,
                                "pathtype": path.path_type,
                                "path": path.path,
                                "svcport": path.backend.service.port.number,
                                "rscapigroup": "",
                                "rsckind": "",
                                "uid": ing.metadata.uid
                            })
                        elif path.backend.resource:
                            ing_host_data.append({
                                "backendtype": "resource",
                                "backendname": path.backend.resource.name,
                                "hostname": hostname,
                                "pathtype": path.path_type,
                                "path": path.path,
                                "svcport": "",
                                "rscapigroup": path.backend.resource.api_group,
                                "rsckind": path.backend.resource.kind,
                                "uid": ing.metadata.uid
                            })

                ing_data[ing.metadata.uid] = {
                    "nsid": 0,
                    "name": ing.metadata.name,
                    "uid": ing.metadata.uid,
                    "starttime": utils.datetime_to_timestampz(ing.metadata.creation_timestamp),
                    "classname": ing.spec.ingress_class_name if ing.spec.ingress_class_name else str(),
                    "label": ing.metadata.labels if ing.metadata.labels else dict(),
                    "nsname": ing.metadata.namespace,
                    "hostdata": ing_host_data
                }

            self.log.write("GET", "Kube Ingress Data Import is completed.")

            self.ing_data = ing_data 
        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_ds_data(self, api):
        try:
            daemonsets = api.list_daemon_set_for_all_namespaces()
            ds_data = dict()

            for ds in daemonsets.items:
                ds_data[ds.metadata.uid] = {
                    "nsid": 0,
                    "name": ds.metadata.name,
                    "uid": ds.metadata.uid,
                    "starttime": utils.datetime_to_timestampz(ds.metadata.creation_timestamp),
                    "serviceaccount": ds.spec.template.spec.service_account,
                    "current": utils.nvl_zero(ds.status.current_number_scheduled),
                    "desired": utils.nvl_zero(ds.status.desired_number_scheduled),
                    "ready": utils.nvl_zero(ds.status.number_ready),
                    "updated": utils.nvl_zero(ds.status.updated_number_scheduled),
                    "available": utils.nvl_zero(ds.status.number_available),
                    "label": ds.metadata.labels if ds.metadata.labels else dict(),
                    "selector": ds.spec.selector.match_labels if ds.spec.selector else dict(),
                    "nsname": ds.metadata.namespace
                }

            self.log.write("GET", "Kube DaemonSet Data Import is completed.")

            self.ds_data = ds_data
        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_rs_data(self, api):
        try:
            replicasets = api.list_replica_set_for_all_namespaces()
            rs_data = dict()

            for rs in replicasets.items:
                rs_data[rs.metadata.uid] = {
                    "nsid": 0,
                    "name": rs.metadata.name,
                    "uid": rs.metadata.uid,
                    "starttime": utils.datetime_to_timestampz(rs.metadata.creation_timestamp),
                    "replicas": utils.nvl_zero(rs.status.replicas),
                    "fullylabeledrs": utils.nvl_zero(rs.status.fully_labeled_replicas),
                    "readyrs": utils.nvl_zero(rs.status.ready_replicas),
                    "availablers": utils.nvl_zero(rs.status.available_replicas),
                    "observedgen": utils.nvl_zero(rs.status.observed_generation),
                    "label": rs.metadata.labels if rs.metadata.labels else dict(),
                    "selector": rs.spec.selector.match_labels if rs.spec.selector else dict(),
                    "refkind": rs.metadata.owner_references[0].kind if rs.metadata.owner_references else "",
                    "refid": 0,                    
                    "refuid": rs.metadata.owner_references[0].uid if rs.metadata.owner_references else 0,
                    "nsname": rs.metadata.namespace
                }

            self.log.write("GET", "Kube ReplicaSet Data Import is completed.")

            self.rs_data = rs_data                
        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_deploy_data(self, api):
        try:
            deployments = api.list_deployment_for_all_namespaces()
            deploy_data = dict()

            for deploy in deployments.items:
                deploy_data[deploy.metadata.uid] = {
                    "nsid": 0,
                    "name": deploy.metadata.name,
                    "uid": deploy.metadata.uid,
                    "starttime": utils.datetime_to_timestampz(deploy.metadata.creation_timestamp),
                    "serviceaccount": deploy.spec.template.spec.service_account,
                    "replicas": utils.nvl_zero(deploy.status.replicas),
                    "updatedrs": utils.nvl_zero(deploy.status.updated_replicas),
                    "readyrs": utils.nvl_zero(deploy.status.ready_replicas),
                    "availablers": utils.nvl_zero(deploy.status.available_replicas),
                    "observedgen": utils.nvl_zero(deploy.status.observed_generation),
                    "label": deploy.metadata.labels if deploy.metadata.labels else dict(),
                    "selector": deploy.spec.selector.match_labels if deploy.spec.selector else dict(),
                    "nsname": deploy.metadata.namespace
                }

            self.log.write("GET", "Kube Deployment Data Import is completed.")

            self.deploy_data = deploy_data                
        except Exception as e:
            self.log.write("Error", str(e))

    def get_kube_sts_data(self, api):
        try:
            statefulsets = api.list_stateful_set_for_all_namespaces()
            sts_data = dict()

            for sts in statefulsets.items:
                sts_data[sts.metadata.uid] = {
                    "nsid": 0,
                    "name": sts.metadata.name,
                    "uid": sts.metadata.uid,
                    "starttime": utils.datetime_to_timestampz(sts.metadata.creation_timestamp),
                    "serviceaccount": sts.spec.template.spec.service_account,
                    "replicas": utils.nvl_zero(sts.status.replicas),
                    "readyrs": utils.nvl_zero(sts.status.ready_replicas),
                    "availablers": utils.nvl_zero(sts.status.available_replicas),
                    "label": sts.metadata.labels if sts.metadata.labels else dict(),
                    "selector": sts.spec.selector.match_labels if sts.spec.selector else dict(),
                    "nsname": sts.metadata.namespace
                }

            self.log.write("GET", "Kube StatefulSet Data Import is completed.")

            self.sts_data = sts_data                
        except Exception as e:
            self.log.write("Error", str(e))