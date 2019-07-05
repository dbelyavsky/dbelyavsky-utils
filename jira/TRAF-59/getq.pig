qlog = load '/user/thresher/quality/logs/2018/11/18/12/*/impr*' using PigStorage('\t');

the_qlog = filter qlog by $37 matches '.*dc3e9d21-5b2d-9113-41fe-84c04afb3949.*';

rmf QLOG.tsv
store the_qlog into 'QLOG.tsv' using PigStorage('\t');

