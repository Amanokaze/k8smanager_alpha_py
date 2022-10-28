SELECT_PG_TABLES_TABLENAME_COUNT_REF = "select count(*) from pg_tables where tablename = '{}'"
SELECT_PG_TABLES_TABLENAME_COUNT_MET = "select count(*) from pg_tables where tablename in ('realtime{0}{1}','avg{0}{1}');"

SELECT_ONTUNEINFO = "select _time from ontuneinfo limit 1"
SELECT_ONTUNEINFO_BIAS = "select _time, _bias from ontuneinfo;"

SELECT_TABLEINFO_TABLENAME = "select count(*) from kubetableinfo where _tablename='{}';"
UPDATE_TABLEINFO = "update kubetableinfo set _updatetime={} where _tablename='{}';"
INSERT_TABLEINFO = "insert into kubetableinfo values ('{1}', 0, {0}, {0}, 0);"

INSERT_TABLE = "insert into {} ({}) values ({});"
SELECT_TABLE = "select {} from {} where {} {};"
UPDATE_TABLE = "update {0} set {1}, _updatetime={4} where {2}={3};"
UPDATE_ENABLED = "update {0} set _enabled=0, _updatetime={3} where {1} in ({2});"
DELETE_TABLE = "delete from {};"
DROP_VIEW = "drop view {};"

SELECT_MANAGERINFO_IP = "select * from kubemanagerinfo where _ip='{}';"
SELECT_CLUSTERINFO_IP_MGRID = "select * from kubeclusterinfo where _ip='{}' and _managerid={};"
SELECT_RESOURCEINFO_CLUSTERID = "select * from kuberesourceinfo where _clusterid={};"
SELECT_SCINFO_CLUSTERID = "select sc.* from kubescinfo sc where sc._clusterid={} and sc._enabled=1;"
SELECT_NAMESPACEINFO_CLUSTERID = "select * from kubensinfo where _clusterid={} and _enabled=1;"
SELECT_NODEINFO_CLUSTERID = "select * from kubenodeinfo where _clusterid={} and _enabled=1;"
SELECT_NODE_SYSCONTAINER_NODEID = "select nsc.*, n._nodename from kubenodesyscoinfo nsc, kubenodeinfo n where nsc._nodeid=n._nodeid and n._nodeid in ({});"
SELECT_REF_CONTAINERINFO_CLUSTERID = "select * from kuberefcontainerinfo ref where _clusterid={} and _enabled=1;"
SELECT_SVCINFO_CLUSTERID= "select svc.* from kubesvcinfo svc, kubensinfo ns where ns._nsid=svc._nsid and ns._clusterid={} and svc._enabled=1;"
SELECT_INGINFO_CLUSTERID= "select ing.* from kubeinginfo ing, kubensinfo ns where ns._nsid=ing._nsid and ns._clusterid={} and ing._enabled=1;"
SELECT_INGHOSTINFO_CLUSTERID= "select h.*, ing._uid as _inguid from kubeinghostinfo h, kubeinginfo ing, kubensinfo ns where h._ingid = ing._ingid and ns._nsid=ing._nsid and ns._clusterid={} and ing._enabled=1;"
SELECT_DEPLOYINFO_CLUSTERID = "select dep.* from kubedeployinfo dep, kubensinfo ns where ns._nsid=dep._nsid and ns._clusterid={} and dep._enabled=1;"
SELECT_STSINFO_CLUSTERID = "select sts.* from kubestsinfo sts, kubensinfo ns where ns._nsid=sts._nsid and ns._clusterid={} and sts._enabled=1;"
SELECT_DSINFO_CLUSTERID = "select ds.* from kubedsinfo ds, kubensinfo ns where ns._nsid=ds._nsid and ns._clusterid={} and ds._enabled=1;"
SELECT_RSINFO_CLUSTERID = "select rs.* from kubersinfo rs, kubensinfo ns where ns._nsid=rs._nsid and ns._clusterid={} and rs._enabled=1;"
SELECT_PVCINFO_CLUSTERID = "select pvc.* from kubepvcinfo pvc, kubensinfo ns where ns._nsid=pvc._nsid and ns._clusterid={} and pvc._enabled=1;"
SELECT_EVENTINFO_CLUSTERID = "select e.* from kubeeventinfo e, kubensinfo ns where ns._nsid=e._nsid and ns._clusterid={} and e._enabled=1;"
SELECT_PODINFO_CLUSTERID = "select p.* from kubepodinfo p, kubenodeinfo n where p._nodeid = n._nodeid and n._clusterid={} and n._enabled=1 and p._enabled=1;"
SELECT_PODINFO_NODEID = "select * from kubepodinfo where _nodeid in ({}) and _enabled=1;"
SELECT_CONTAINERINFO_CLUSTERID = "select c.*, p._uid as _poduid from kubecontainerinfo c, kubepodinfo p, kubenodeinfo n where c._podid=p._podid and p._nodeid=n._nodeid and n._clusterid={} and n._enabled=1 and p._enabled=1 and c._enabled=1 order by p._podid, c._containername;"
SELECT_CONTAINERINFO_NODEID = "select c.*, p._uid as _poduid, n._nodename from kubecontainerinfo c, kubepodinfo p, kubenodeinfo n where c._podid=p._podid and p._nodeid=n._nodeid and p._enabled=1 and p._nodeid in ({});"
SELECT_PODDEVICEINFO_DEVICETYPE = "select * from kubepoddeviceinfo where _devicetype='{}';"
SELECT_NODEINFO_MGRIP = "select n.* from kubenodeinfo n, kubeclusterinfo c, kubemanagerinfo m where m._managerid = c._managerid and c._clusterid = n._clusterid and m._managerid = n._managerid and n._enabled=1 and m._ip='{}';"
SELECT_LABELVALUEINFO = "select * from kubelabelvalueinfo;"
SELECT_LABELINFO = "select l.*, lv._keyvalue from kubelabelinfo l, kubelabelvalueinfo lv where lv._lbvalueid = l._lbvalueid and l._enabled=1;"
SELECT_SELECTORINFO = "select s.*, lv._keyvalue from kubeselectorinfo s, kubelabelvalueinfo lv where lv._lbvalueid = s._lbvalueid and s._enabled=1;"

SELECT_SVC_POD_MAPPING = """
select t.*
  from (select skind, sid, suid, sname, 
			   array_to_string(array_agg(slbid order by slbid),',') as slbids,
			   lkind, lid, luid, lname,
			   array_to_string(array_agg(llbid order by llbid),',') as llbids,
			   case when sum(case when status = 'Running' then 1 else 0 end) > 0 then 'Running' else 'Failed' end runningstatus
		  from (select s._kind skind, v._svcid sid, s._kinduid suid, v._svcname sname, s._lbvalueid slbid, l._kind lkind, p._podid lid, l._kinduid luid, p._podname lname, l._lbvalueid llbid, p._status status
				  from kubeselectorinfo s, kubelabelinfo l, kubepodinfo p, kubesvcinfo v
				 where l._enabled=1 and s._enabled=1 and p._enabled=1 and v._enabled=1
		           and l._kind='Pod' and s._kind='Service'
		           and l._kinduid = p._uid
		           and s._kinduid = v._uid
				   and l._lbvalueid=s._lbvalueid) t
		 group by skind, sid, suid, sname, lkind, lid, luid, lname) t
 where t.slbids = t.llbids;
"""

SELECT_ING_SVC_POD_MAPPING = """
select ingsvc.*, svcpod.lid, svcpod.luid, svcpod.lname, svcpod.runningstatus
  from (select t.*
		  from (select skind, sid, suid, sname, 
					   array_to_string(array_agg(slbid order by slbid),',') as slbids,
					   lkind, lid, luid, lname,
					   array_to_string(array_agg(llbid order by llbid),',') as llbids,
					   case when sum(case when status = 'Running' then 1 else 0 end) > 0 then 'Running' else 'Failed' end runningstatus
				  from (select s._kind skind, v._svcid sid, s._kinduid suid, v._svcname sname, s._lbvalueid slbid, l._kind lkind, p._podid lid, l._kinduid luid, p._podname lname, l._lbvalueid llbid, p._status status
						  from kubeselectorinfo s, kubelabelinfo l, kubepodinfo p, kubesvcinfo v
						 where l._enabled=1 and s._enabled=1 and p._enabled=1 and v._enabled=1
				           and l._kind='Pod' and s._kind='Service'
				           and l._kinduid = p._uid
				           and s._kinduid = v._uid
						   and l._lbvalueid=s._lbvalueid) t
				 group by skind, sid, suid, sname, lkind, lid, luid, lname) t
		 where t.slbids = t.llbids) svcpod,
		(select i._ingid iid, i._uid iuid, i._ingname, s._svcid sid, s._uid suid, s._svcname sname
		  from kubeinginfo i, kubeinghostinfo ih, kubesvcinfo s
		 where i._ingid = ih._ingid
           and i._enabled=1 and ih._enabled=1
		   and ih._backendtype = 'service'
		   and ih._backendname = s._svcname) ingsvc
 where ingsvc.suid = svcpod.suid;
"""

STAT_POD_COLUMNS = """
	   coalesce(sum(p._cpuusage),0) _cpuusage,
	   case when sum(p._memorysize) = 0 then 0 else coalesce(round(sum(p._workingsetsize) / sum(p._memorysize) * 100 * 100),0) end _memoryused,
	   case when sum(p._memorysize) = 0 then 0 else coalesce(round(sum(p._swapsize) / sum(p._memorysize) * 100 * 100),0) end _swapused,
	   coalesce(sum(p._memorysize),0) _memorysize,
	   coalesce(sum(p._workingsetsize),0) _workingsetsize,
	   coalesce(sum(p._availablesize),0) _availablesize,
	   coalesce(sum(p._swapsize),0) _swapsize
"""

STAT_POD_SUB_QUERY = """
  from (select l._cpuusage, 
	     			  case when l._memorysize = -1 then 0 else l._workingsetsize end _workingsetsize,
	     			  case when l._memorysize = -1 then 0 else l._swapsize end _swapsize,
	     			  case when l._memorysize = -1 then 0 else l._memorysize end _memorysize,
	     			  case when l._memorysize = -1 then 0 else l._availablesize end _availablesize,
	     			  p._podid, p._refkind, p._refid
		  from kubepodinfo p, kubelastpodrealtimeperf l
		 where p._podid = l._podid
		   and p._enabled=1) p
"""

STAT_NS_METRIC_DATA = f"""
select n._nsid, n._nsname,
       {STAT_POD_COLUMNS}
  from (select l._cpuusage, 
	     			  case when l._memorysize = -1 then 0 else l._workingsetsize end _workingsetsize,
	     			  case when l._memorysize = -1 then 0 else l._swapsize end _swapsize,
	     			  case when l._memorysize = -1 then 0 else l._memorysize end _memorysize,
	     			  case when l._memorysize = -1 then 0 else l._availablesize end _availablesize,
	     			  p._nsid
		  from kubepodinfo p, kubelastpodrealtimeperf l
		 where p._podid = l._podid
		   and p._enabled=1) p
 right outer join kubensinfo n
    on p._nsid = n._nsid
   and n._status = 'Active'
   and n._enabled=1
 group by n._nsid, n._nsname
 order by n._nsid;
"""

STAT_ING_METRIC_DATA = f"""
select v.sid, v.sname,
       {STAT_POD_COLUMNS}
{STAT_POD_SUB_QUERY}
 right outer join kubeingpodmappingv v
    on p._podid = v.lid
   and v.runningstatus = 'Running'
 group by v.sid, v.sname
 order by v.sid;
"""

STAT_SVC_METRIC_DATA = f"""
select v.sid, v.sname,
       {STAT_POD_COLUMNS}
{STAT_POD_SUB_QUERY}
 right outer join kubesvcpodmappingv v
    on p._podid = v.lid
   and v.runningstatus = 'Running'
 group by v.sid, v.sname
 order by v.sid;
"""

STAT_DEPLOY_METRIC_DATA = f"""
select d._deployid, d._deployname,
       {STAT_POD_COLUMNS}
{STAT_POD_SUB_QUERY}
 right outer join (select d._deployid, d._deployname, d._uid, r._rsid
                     from kubedeployinfo d, kubersinfo r
                    where r._refkind = 'Deployment'
                      and r._refid = d._deployid 
				              and d._replicas > 0
				              and d._availablers = d._replicas
                      and r._enabled=1 and d._enabled=1) d
    on p._refkind = 'ReplicaSet'
   and p._refid = d._rsid 
 group by d._deployid, d._deployname
 order by d._deployid;
"""

STAT_STS_METRIC_DATA = f"""
select s._stsid, s._stsname,
       {STAT_POD_COLUMNS}
{STAT_POD_SUB_QUERY}
 right outer join kubestsinfo s 
    on p._refkind = 'StatefulSet'
   and p._refid = s._stsid
   and s._enabled = 1   
   and s._replicas > 0
   and s._availablers = s._replicas
 group by s._stsid, s._stsname
 order by s._stsid;
"""

STAT_DS_METRIC_DATA = f"""
select ds._dsid, ds._dsname,
       {STAT_POD_COLUMNS}
{STAT_POD_SUB_QUERY}
 right outer join kubedsinfo ds 
    on p._refkind = 'DaemonSet'
   and p._refid = ds._dsid
   and ds._enabled = 1   
   and ds._desired > 0
   and ds._available = ds._desired
 group by ds._dsid, ds._dsname
 order by ds._dsid;
"""

STAT_RS_METRIC_DATA = f"""
select rs._rsid, rs._rsname,
       {STAT_POD_COLUMNS}
{STAT_POD_SUB_QUERY}
 right outer join kubersinfo rs 
    on p._refkind = 'ReplicaSet'
   and p._refid = rs._rsid
   and rs._enabled = 1   
   and rs._replicas > 0
   and rs._availablers = rs._replicas
 group by rs._rsid, rs._rsname
 order by rs._rsid;
"""

SELECT_VIEWER_OVERALL = """SELECT n._nodename, p.*
  FROM kubelastnoderealtimeperf p, kubenodeinfo n
 WHERE n._nodeid = p._nodeid and n._nodeid in ({})
 ORDER BY n._nodename;"""

SELECT_VIEWER_NODESYSCO = """SELECT n._nodename, nsc._containername, rnsc.*
  FROM realtimekubenodesysco_{0} rnsc, kubenodeinfo n, kubenodesyscoinfo nsc,
       (select _updatetime from kubetableinfo where _tablename='realtimekubenodesysco_{0}') t
 WHERE n._nodeid = rnsc._nodeid and n._nodeid = {1}
   AND rnsc._ontunetime = t._updatetime
   and rnsc._syscontainerid = nsc._syscontainerid
 ORDER BY nsc._containername;"""

SELECT_VIEWER_POD = """SELECT p._podname, rp.*
  FROM realtimekubepodperf_{0} rp, kubepodinfo p,
       (select _updatetime from kubetableinfo where _tablename='realtimekubepodperf_{0}') t
 WHERE p._podid = rp._podid and p._nodeid = {1}
   AND rp._ontunetime = t._updatetime
 ORDER BY p._podid;"""

SELECT_VIEWER_CONTAINER = """SELECT c._containername, rcp.*
  FROM realtimekubecontainerperf_{0} rcp, kubecontainerinfo c,
       (select _updatetime from kubetableinfo where _tablename='realtimekubecontainerperf_{0}') t
 WHERE c._containerid = rcp._containerid and c._podid = {1}
   AND rcp._ontunetime = t._updatetime
 ORDER BY c._podid;"""

SELECT_VIEWER_PODNET = """SELECT d._devicename, rpn.*
  FROM realtimekubepodnet_{0} rpn, kubepoddeviceinfo d,
       (select _updatetime from kubetableinfo where _tablename='realtimekubepodnet_{0}') t
 WHERE d._deviceid = rpn._deviceid and rpn._podid = {1}
   AND rpn._ontunetime = t._updatetime
 ORDER BY d._devicename;"""

SELECT_VIEWER_PODVOL = """SELECT d._devicename, rpv.*
  FROM realtimekubepodvol_{0} rpv, kubepoddeviceinfo d,
       (select _updatetime from kubetableinfo where _tablename='realtimekubepodvol_{0}') t
 WHERE d._deviceid = rpv._deviceid and rpv._podid = {1}
   AND rpv._ontunetime = t._updatetime
 ORDER BY d._devicename;"""

SELECT_VIEWER_PODTS = """SELECT 
        p._podname, t._ontunetime, t._agenttime, t._cpuusage, t._memoryused
  FROM (SELECT m.* FROM realtimekubepodperf_{0} m, kubepodinfo i where m._podid=i._podid and i._nodeid={2} and m._ontunetime>={3}
        UNION ALL
        SELECT m.* FROM realtimekubepodperf_{1} m, kubepodinfo i where m._podid=i._podid and i._nodeid={2} and m._ontunetime>={3}) t,
       kubepodinfo p
 WHERE p._podid = t._podid
 ORDER BY t._podid, t._ontunetime"""

SELECT_VIEWER_PODTS2 = """SELECT 
        p._podname, t._ontunetime, t._agenttime, t._cpuusage, t._memoryused
  FROM (SELECT m.* FROM realtimekubepodperf_{0} m, kubepodinfo i where m._podid=i._podid and i._nodeid={1} and m._ontunetime>={2}) t,
       kubepodinfo p
 WHERE p._podid = t._podid
 ORDER BY t._podid, t._ontunetime"""