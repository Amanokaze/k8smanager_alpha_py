-- 이거 보고 알아서 새로 만드셈??

CREATE TABLE public.kubetableinfo (
	"_tablename" bpchar(64) NOT NULL primary key,
	"_version" int4 NOT NULL,
	"_createdtime" int4 NOT NULL,
	"_updatetime" int4 NOT NULL,
	"_durationmin" int4 NULL
);

CREATE TABLE public.kubemanagerinfo (
	"_managerid" serial NOT NULL primary key,
	"_managername" text NOT NULL,
	"_desc" text NULL,
	"_ip" text NOT NULL
);

CREATE TABLE public.kubeclusterinfo (
	"_clusterid" serial NOT NULL primary key,
	"_managerid" int4 NOT NULL,
	"_clustername" text NOT NULL,
	"_desc" text NULL,
	"_ip" text NOT NULL
);

CREATE TABLE public.kubenodeinfo (
	"_nodeid" serial NOT NULL primary key,
	"_managerid" int4 NOT NULL,
	"_clusterid" int4 NOT NULL,
	"_nodeuid" text NOT NULL,
	"_nodename" text NULL,
	"_nodenameext" text NULL,
	"_enabled" int4 NOT NULL,
	"_state" int4 NOT NULL,
	"_connected" int4 NOT NULL,
	"_osimage" text NULL,
	"_osname" text NULL,
	"_containerruntimever" text NULL,
	"_kubeletver" text NULL,
	"_kubeproxyver" text NULL,
	"_cpuarch" text NULL,
	"_cpucount" int4 NULL,
	"_ephemeralstorage" int4 NULL,
	"_memorysize" int4 NULL,
	"_pods" int4 NULL,
	"_ip" text NULL
);

create table public.kubensinfo (
	"_nsid" serial not null primary key,
	"_clusterid" int4 not null,
	"_nsname" text not null	
);

create table public.kubepodinfo (
	"_podid" serial not null primary key,
	"_nodeid" int4 not null,
	"_nsid" int4 not null,
	"_uid" text not null,
	"_podname" text not null
);

create table public.kubecontainerinfo (
	"_containerid" serial not null primary key,
	"_podid" int4 not null,
	"_containername" text not null,
	"_starttime" int4
);

create table public.kubenodesyscoinfo (
	"_syscontainerid" serial not null primary key,
	"_containername" text not null
);

create table public.kubepoddeviceinfo (
	"_deviceid" serial not null primary key,
	"_devicename" text not null,
	"_devicetype" text not null
);

CREATE TABLE public.kubelastrealtimeperf (
	"_nodeid" serial not null primary key,
	"_ontunetime" int4 NOT NULL,
	"_cpuusage" int4 NULL,
	"_memavailable" int4 NULL,
	"_memusage" int4 NULL,
	"_memws" int4 NULL,
	"_memrss" int4 NULL,
	"_netusage" int4 NULL,
	"_fsavailable" int4 NULL,
	"_fsused" int4 NULL,
	"_imgfsavailable" int4 NULL,
	"_imgfsused" int4 NULL,
	"_proccount" int4 NULL
);

CREATE TABLE public.realtimekubenodeperf_22062900 (
	"_nodeid" serial not null primary key,
	"_ontunetime" int4 NOT NULL,
	"_agenttime" int4 NOT NULL,
	"_cpuusage" int4 NULL,
	"_memavailable" int4 NULL,
	"_memusage" int4 NULL,
	"_memworkingset" int4 NULL,
	"_memrss" int4 NULL,
	"_mempagefault" int4 NULL,
	"_memmajorpagefault" int4 NULL,
	"_netusage" int4 NULL,
	"_netrxrate" int4 NULL,
	"_nettxrate" int4 NULL,
	"_netrxerrors" int4 NULL,
	"_nettxerrors" int4 NULL,
	"_fsavailable" int4 NULL,
	"_fscapacity" int4 NULL,
	"_fsused" int4 NULL,
	"_fsinode" int4 NULL,
	"_fsinodefree" int4 NULL,
	"_fsinodeused" int4 NULL,
	"_imgfsavailable" int4 NULL,
	"_imgfscapacity" int4 NULL,
	"_imgfsused" int4 NULL,
	"_imgfsinode" int4 NULL,
	"_imgfsinodefree" int4 NULL,
	"_imgfsinodeused" int4 NULL,
	"_maxpid" int4 NULL,
	"_proccount" int4 NULL
)
WITH (
	autovacuum_enabled=false
);
CREATE INDEX irealtimekubenodeperf_22062900 ON public.realtimekubenodeperf_22062900 USING btree (_nodeid, _ontunetime, _agenttime);

CREATE TABLE public.realtimekubenodesysco_22062900 (
	"_nodeid" int4 NOT NULL,
	"_syscontainerid" int4 NOT NULL,
	"_ontunetime" int4 NOT NULL,
	"_agenttime" int4 NOT NULL,
	"_cpuusage" int4 NULL,
	"_memavailable" int4 NULL,
	"_memusage" int4 NULL,
	"_memworkingset" int4 NULL,
	"_memrss" int4 NULL,
	"_mempagefault" int4 NULL,
	"_memmajorpagefault" int4 NULL
)
WITH (
	autovacuum_enabled=false
);
CREATE INDEX irealtimekubenodesysco_22062900 ON public.realtimekubenodesysco_22062900 USING btree (_nodeid, _ontunetime, _agenttime, _syscontainerid);

CREATE TABLE public.realtimekubepodperf_22062900 (
	"_podid" int4 NOT NULL,
	"_ontunetime" int4 NOT NULL,
	"_agenttime" int4 NOT NULL,
	"_cpuusage" int4 NULL,
	"_memavailable" int4 NULL,
	"_memusage" int4 NULL,
	"_memworkingset" int4 NULL,
	"_memrss" int4 NULL,
	"_mempagefault" int4 NULL,
	"_memmajorpagefault" int4 NULL,
	"_netusage" int4 NULL,
	"_netrxrate" int4 NULL,
	"_nettxrate" int4 NULL,
	"_netrxerrors" int4 NULL,
	"_nettxerrors" int4 NULL,
	"_volavailable" int4 NULL,
	"_volcapacity" int4 NULL,
	"_volused" int4 NULL,
	"_volinode" int4 NULL,
	"_volinodefree" int4 NULL,
	"_volinodeused" int4 NULL,
	"_epstavailable" int4 NULL,
	"_epstcapacity" int4 NULL,
	"_epstused" int4 NULL,
	"_epstinode" int4 NULL,
	"_epstinodefree" int4 NULL,
	"_epstinodeused" int4 NULL
)
WITH (
	autovacuum_enabled=false
);
CREATE INDEX irealtimekubepodperf_22062900 ON public.realtimekubepodperf_22062900 USING btree (_podid, _ontunetime, _agenttime);

CREATE TABLE public.realtimekubecontainerperf_22062900 (
	"_containerid" int4 NOT NULL,
	"_ontunetime" int4 NOT NULL,
	"_agenttime" int4 NOT NULL,
	"_cpuusage" int4 NULL,
	"_memavailable" int4 NULL,
	"_memusage" int4 NULL,
	"_memworkingset" int4 NULL,
	"_memrss" int4 NULL,
	"_mempagefault" int4 NULL,
	"_memmajorpagefault" int4 NULL,
	"_rootfsavailable" int4 NULL,
	"_rootfscapacity" int4 NULL,
	"_rootfsused" int4 NULL,
	"_rootfsinode" int4 NULL,
	"_rootfsinodefree" int4 NULL,
	"_rootfsinodeused" int4 NULL,
	"_logfsavailable" int4 NULL,
	"_logfscapacity" int4 NULL,
	"_logfsused" int4 NULL,
	"_logfsinode" int4 NULL,
	"_logfsinodefree" int4 NULL,
	"_logfsinodeused" int4 NULL
)
WITH (
	autovacuum_enabled=false
);
CREATE INDEX irealtimekubecontainerperf_22062900 ON public.realtimekubecontainerperf_22062900 USING btree (_containerid, _ontunetime, _agenttime);

CREATE TABLE public.realtimekubepodnet_22062900 (
	"_podid" int4 NOT NULL,
	"_deviceid" int4 NOT NULL,
	"_ontunetime" int4 NOT NULL,
	"_agenttime" int4 NOT NULL,
	"_netusage" int4 NULL,
	"_netrxrate" int4 NULL,
	"_nettxrate" int4 NULL,
	"_netrxerrors" int4 NULL,
	"_nettxerrors" int4 NULL
)
WITH (
	autovacuum_enabled=false
);
CREATE INDEX irealtimekubepodnet_22062900 ON public.realtimekubepodnet_22062900 USING btree (_podid, _ontunetime, _agenttime, _deviceid);

CREATE TABLE public.realtimekubepodvol_22062900 (
	"_podid" int4 NOT NULL,
	"_deviceid" int4 NOT NULL,
	"_ontunetime" int4 NOT NULL,
	"_agenttime" int4 NOT NULL,
	"_volavailable" int4 NULL,
	"_volcapacity" int4 NULL,
	"_volused" int4 NULL,
	"_volinode" int4 NULL,
	"_volinodefree" int4 NULL,
	"_volinodeused" int4 NULL
)
WITH (
	autovacuum_enabled=false
);
CREATE INDEX irealtimekubepodvol_22062900 ON public.realtimekubepodvol_22062900 USING btree (_podid, _ontunetime, _agenttime, _deviceid);
