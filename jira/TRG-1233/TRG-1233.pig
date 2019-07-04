qlog = load '/user/thresher/quality/logs/2017/10/30/12/impressions/*' using PigStorage('\t');
qlog_filtered = filter qlog by ($9 matches '.*give.salvationarmyusa.org.*' and $57 matches '.*t[0-9].*');
rmf TRG-1233.201710301200
store qlog_filtered into 'TRG-1233.201710301200' using PigStorage('\t');


event = load '/user/thresher/event/2017/10/30/12/00/*' using PigStorage('\t');
event_filtered = filter event
	by ($36 matches '.*id=(88e7208d-8e12-179d-1836-1ed4e53f72cd|7c01ed26-1846-c263-398b-d5a32096075c|b8d74070-85da-b272-32a6-41928faa0a66),.*');
rmf TRG-1233.event.201710301200
store event_filtered into 'TRG-1233.event.201710301200' using PigStorage('\t');



dt = load '/user/thresher/dt/raw/2017/10/30/12/00/*' using PigStorage('\t');
dt_filtered = filter dt
	by ($8 matches '(88e7208d-8e12-179d-1836-1ed4e53f72cd|7c01ed26-1846-c263-398b-d5a32096075c|b8d74070-85da-b272-32a6-41928faa0a66)');
rmf TRG-1233.dt.201710301200
store dt_filtered into 'TRG-1233.dt.201710301200' using PigStorage('\t');
