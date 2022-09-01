from datetime import datetime
from kubernetes import client, config
import time
import json

NANO = 1000000000
MEGA = 1000000
MILLI = 1000

config = client.Configuration()
config.api_key['authorization'] = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IlRyYzA4SjdEZUloZGM0M0pvODRJOTVqUngzdTB6dzE1RXBGVWQ1NV9qZEkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJhZG1pbi11c2VyLXRva2VuLXFkOGY1Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImFkbWluLXVzZXIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiIyYTJmYWMzZi1kOWQxLTQ3MmUtODdmMi0xYjNmZDQ5ZjQxMTEiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06YWRtaW4tdXNlciJ9.HygvQ7eaenNsMiohsR4rButEQjwecXFF_9OwrKQI_yMN1DeMuFqtRq0jk_-bvaY8kT3AXY2uekkIpXusw5C1D0KX-WDUpfVRa7OO53JEbTfJBE-Ki4md3v7aJTni_lyORJoZ45ziKbofc2z0_g87e3U2xeJtVOFZTdjniIMFCHkgl8qF2wDK5MN0WapGbnt8tj3nxdn4vlnK8fpp66GkvQ8-x3TFOdD-koqVl5JGg9Q-K72V_fCFplcyBUcaogBNUCXLpf2Ck4i-kUtTDouWKmw8klqXCbCCVotTHQk13A4EFA-0n9Gm-ihW-3ANuNP9t3F_ZJqz8FRxjEuUIlI7nw'
config.api_key_prefix['authorization'] = 'Bearer'
config.host = 'https://192.168.0.138:6443'
config.verify_ssl = True
config.ssl_ca_cert = 'ca.crt'

def create_file(name):
    file_name = f"result/{name}.log"
    file = open(file_name, "wt", encoding="utf8")
    file.close()

def write_file(name, data):
    file_name = f"result/{name}.log"
    file = open(file_name, "a", encoding="utf8")
    file.write(str(data))
    file.write("\n")
    file.close()

def conv(value, per):
    conv_val = 0
    if per == "MByte" or per == "mb" or per == "M":
        conv_val = round(value / MEGA)
    elif per == "s":
        conv_val = round(value / NANO)
    elif per == "kb":
        conv_val = round(value / MILLI)
    else:
        conv_val = round(value / NANO * MILLI)

    return f"{str(conv_val)} {per}"

while True:
    api = client.CoreV1Api(client.ApiClient(config))
    nodes = api.list_node()
    nodes_str = "nodes"
    stats_str = "stats"
    metric_str = "metric"

    create_file(nodes_str)
    create_file(stats_str)
    create_file(metric_str)

    write_file(nodes_str, f"({datetime.now()})")
    write_file(nodes_str, "--- Node List---")
    for node in nodes.items:
        write_file(nodes_str, f"# {node.metadata.name} #")
        write_file(nodes_str, f"-- Hostname: {list(x.address for x in node.status.addresses if x.type == 'Hostname')[0]}")
        write_file(nodes_str, f"-- Address: {list(x.address for x in node.status.addresses if x.type == 'InternalIP')[0]}")
        write_file(nodes_str, f"-- UID: {node.metadata.uid}")
        write_file(nodes_str, f"-- Capacity / Allocatable")
        write_file(nodes_str, f"---- CPU: {node.status.capacity['cpu']} / {node.status.allocatable['cpu']}")
        write_file(nodes_str, f"---- Storage: {node.status.capacity['ephemeral-storage']} / {node.status.allocatable['ephemeral-storage']}")
        write_file(nodes_str, f"---- Memory: {node.status.capacity['memory']} / {node.status.allocatable['memory']}")
        write_file(nodes_str, f"---- Pods: {node.status.capacity['pods']} / {node.status.allocatable['pods']}")

        nodename = node.metadata.name

        node_stats = api.connect_get_node_proxy_with_path(nodename, "stats/summary")
        node_metric_rs = api.connect_get_node_proxy_with_path(nodename, "metrics/resource")
        node_metric_ca = api.connect_get_node_proxy_with_path(nodename, "metrics/cadvisor")

        node_stats_json = json.loads(node_stats.replace("'",'"'))
        node_info = node_stats_json['node']
        write_file(stats_str, f"# {nodename} Kubelet API - stats/summary")
        write_file(stats_str, f"-- Runtime: {node_info['startTime']}")
        write_file(stats_str, f"-- CPU Usage Nanocore / CoreNanosecs: {conv(node_info['cpu']['usageNanoCores'],'mcore')} / {conv(node_info['cpu']['usageCoreNanoSeconds'],'s')}")
        write_file(stats_str, f"-- Memory")
        write_file(stats_str, f"---- Available: {conv(node_info['memory']['availableBytes'],'MByte')}")
        write_file(stats_str, f"---- Usage: {conv(node_info['memory']['usageBytes'],'MByte')}")
        write_file(stats_str, f"---- WorkingSet: {conv(node_info['memory']['workingSetBytes'],'MByte')}")
        write_file(stats_str, f"---- RSS: {conv(node_info['memory']['rssBytes'],'MByte')}")
        write_file(stats_str, f"---- PageFaults: {conv(node_info['memory']['pageFaults'],'M')}")
        write_file(stats_str, f"---- MajorPageFaults: {node_info['memory']['majorPageFaults']}")

        # 80L
        pods_info = node_stats_json['pods']
        write_file(stats_str, f"## {nodename} Pods metric data")
        write_file(stats_str, f"-- Containers CPU, Memory, Filesystem can see but do not write because of too complex")
        write_file(stats_str, f"-- Network total usage is not provided but separate adapter usage is provided, so network usage is sum the adapter's usage.")
        write_file(stats_str, f"-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
        write_file(stats_str, f"                          name                          |      namespace     |cpu(mc)|usmem(mb)|wsmem(mb)|rssmem(mb)|ntrx(mb)|ntrxerr|nttx(mb)|nttxerr|avfs(mb)|usfs(kb)|prccnt")
        write_file(stats_str, f"-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

        for pod in pods_info:
            nt_total = {
                "rxbytes": conv(sum(x["rxBytes"] for x in pod["network"]["interfaces"]),"mb"),
                "rxerrors": sum(x["rxErrors"] for x in pod["network"]["interfaces"]),
                "txbytes": conv(sum(x["txBytes"] for x in pod["network"]["interfaces"]),"mb"),
                "txerrors": sum(x["txErrors"] for x in pod["network"]["interfaces"])
            }
            write_file(stats_str, "{0:<56}|{1:<20}|{2:>7}|{3:>9}|{4:>9}|{5:>10}|{6:>8}|{7:>7}|{8:>8}|{9:>7}|{10:>8}|{11:>8}|{12:>6}".format(
                pod['podRef']['name'],
                pod['podRef']['namespace'],
                conv(pod['cpu']['usageNanoCores'],'mc'),
                conv(pod['memory']['usageBytes'],'mb'),
                conv(pod['memory']['workingSetBytes'],'mb'),
                conv(pod['memory']['rssBytes'],'mb'),
                nt_total['rxbytes'],
                nt_total['rxerrors'],
                nt_total['txbytes'],
                nt_total['txerrors'],
                conv(pod['ephemeral-storage']['availableBytes'],'mb'),
                conv(pod['ephemeral-storage']['usedBytes'],'kb'),
                pod['process_stats']['process_count']
            ))


        write_file(stats_str, "")

        # Stat Summary 다 했으니 이제 Metric을 해봅시다..
        
    # file_name = "result/debug.log"
    # file = open(file_name, "wt", encoding="utf8")
    # file.write(str(nodes))
    # file.write("\n")
    # file.close()

    # api_client = client.CoreV1Api(client.ApiClient(config))

    # ret = api_client.list_namespaced_pod("monitoring", watch=False)


    # for i in ret.items:
    #     print(f"{i.status.pod_ip}\t{i.metadata.name}")

    # api_client2 = client.CustomObjectsApi(client.ApiClient(config))
    # nodes = api_client2.list_cluster_custom_object("metrics.k8s.io","v1beta1","nodes")
    # pods = api_client2.list_cluster_custom_object("metrics.k8s.io","v1beta1","pods")
    # pods2 = api_client2.list_namespaced_custom_object("metrics.k8s.io","v1beta1","monitoring","pods")

    # pod = api_client2.get_cluster_custom_object("metrics.k8s.io","v1beta1","nodes","node03")
    # pod2 = api_client2.get_namespaced_custom_object("metrics.k8s.io","v1beta1","monitoring","pods","grafana-prometheus-77cb5f8495-v8xzz")

    # node03 = api_client.connect_get_node_proxy_with_path("node03","stats/summary")

    # file_name = "result/kubelet.log"
    # file = open(file_name, "wt", encoding="utf8")
    # file.write(str(node03))
    # file.write("\n")
    # file.close()

    time.sleep(5)
