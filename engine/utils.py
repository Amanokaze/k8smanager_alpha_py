import time
from datetime import datetime, timedelta

ONE_DAY_SECONDS = 86400

def change_quantity_unit(value):
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

def dict_to_str(data):
    return ",".join(list(f"{k}={v}" for (k,v) in data.items())) if data else ""

def dict_port_to_str(ports):
    return ",".join(list(f"{x.port}" + "" if x.port == x.target_port else f":{x.target_port}" + f"/{x.protocol}" for x in ports)) if ports else ""

def insert_columns_ref(schema, obj_name):
    return ",".join(list(y[0] for y in list(filter(lambda x: 'PRIMARY KEY' not in x, schema["reference"][obj_name]))))

def insert_columns_metric(schema, obj_name):
    metric_list = list(schema["reference"][obj_name]) if obj_name == "kubelastrealtimeperf" else list(schema["metric"][obj_name])
    return ",".join(list(y[0] for y in metric_list))

def insert_values(data):
    return ",".join(list(f"'{x}'" for x in data))

def select_average_columns(schema, obj_name, key_columns):
    not_averaged_columns = key_columns + ['_ontunetime','_agenttime']
    metric_list = list(x[0] for x in schema["metric"][obj_name] if x[0] not in not_averaged_columns)
    return ",".join(key_columns + list(f"round(avg({x}))::int8 {x}" for x in metric_list))

def update_values(schema, obj_name, data):
    not_included_column_list = ("_managerid","_clusterid")
    column_list = list(y[0] for y in list(filter(lambda x: 'PRIMARY KEY' not in x and x[0] not in not_included_column_list, schema["reference"][obj_name])))
    value_list = list(data.values())
    stmt_list = list(f"{column_list[i]}='{value_list[i]}'" for i in range(len(column_list)))

    return ",".join(stmt_list)

def svtime_to_timestampz(svtime):
    tz_interval = ONE_DAY_SECONDS - int(time.mktime(time.gmtime(ONE_DAY_SECONDS)))
    return int(time.mktime((datetime.strptime(svtime, '%Y-%m-%dT%H:%M:%SZ') + timedelta(seconds=tz_interval)).timetuple()))

def datetime_to_timestampz(dtvalue):
    return int(datetime.timestamp(dtvalue))

def calculate(cal_type, values):
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

def nvl_zero(values):
    return values if values else 0

def nvl_str(values):
    return values if values else str()

def msg_str(values):
    return values.replace("'","''")