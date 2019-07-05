event = load '/user/thresher/event/2018/11/18/12/*/LOG*app{69,02}.*' using PigStorage('\t');

the_event = filter event by $36 matches '.*dc3e9d21-5b2d-9113-41fe-84c04afb3949.*';

rmf LOG.tsv
store the_event into 'LOG.tsv' using PigStorage('\t');

