#!/bin/bash

DATE=2017/02/22
DATE_AFTER=2017/02/23
DATESTR=${DATE//\//}
DATESTR_AFTER=${DATE_AFTER//\//}
DATEMO=${DATE%%???}
DATEMOSTR=${DATEMO//\//}

# copy rs3
#hdfs dfs -mkdir -p aggdt/rs3/${DATEMO}
#hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM} -m 200 /user/thresher/aggdt/rs3/${DATE} aggdt/rs3/${DATEMO}

# copy marting data over from production
#hdfs dfs -rm -r mart/*/${DATE}/*
#hdfs dfs -mkdir -p mart/agency/${DATEMO}
#hdfs dfs -mkdir -p mart/network/${DATEMO}
#hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM} -m 200 /user/thresher/mart/agency/${DATE} mart/agency/${DATEMO}
#echo hadoop distcp -D mapreduce.map.memory.mb=2024 -log /tmp/${RANDOM} -m 200 /user/thresher/mart/network/${DATE} mart/network/${DATEMO}
# or if it's already archived
#hadoop distcp har:///user/thresher/mart/network/${DATE}.har/* hdfs:/user/${USER}/mart/network/${DATEMO}
#hadoop distcp har:///user/thresher/mart/agency/${DATE}.har/* hdfs:/user/${USER}/mart/agency/${DATEMO}

# remove some qlog related marting output then rerun marting
for martData in \
	agg_agency_quality_V3\
	agg_agency_quality_pre\
	agg_agency_quality_pre_V3\
	agg_agency_quality_site_V3\
	agg_agency_quality_site_pre\
	agg_agency_quality_site_pre_V3\
	agg_agency_quality_placement_billable_V3\
;do
	#hdfs dfs -rm -r mart/agency/${DATE}/${DATESTR_AFTER}*/$martData
	echo -n
done

for martData in \
	agg_network_quality_V3\
	agg_network_quality_V3_pub\
	agg_network_quality_pre\
	agg_network_quality_pre_V3\
	agg_network_quality_pre_V3_pub\
	agg_network_quality_site_V3\
	agg_network_quality_site_pre\
	agg_network_quality_site_pre_V3\
;do
	#hdfs dfs -rm -r mart/network/${DATE}/${DATESTR_AFTER}*/$martData
	echo -n
done

#hdfs dfs -rm -r mart/*/${DATE}/${DATESTR}*/rsa_metrics_mean

#hdfs dfs -mkdir -p scale_factors/${DATE}
#hdfs dfs -touchz scale_factors/${DATE}/groupm_scale_factors

#rm ~/ib02dataload/*/*
#hdfs dfs -rm status/aggcontroller/*/*
#hdfs dfs -mkdir -p status/aggcontroller/todo
#hdfs dfs -touchz status/aggcontroller/todo/${DATESTR}.${DATESTR_AFTER}003501

~/ias_mart/bin/aggregation_controller.sh --loop
