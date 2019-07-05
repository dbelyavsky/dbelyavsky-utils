dt = load '/user/thresher/dt/raw/2018/11/18/12/*/DT*' using PigStorage('\t');

the_dt = filter dt by ($8 == '65272439-31e3-b778-dced-88350958af04' OR $8 == 'dc3e9d21-5b2d-9113-41fe-84c04afb3949');

rmf DT.tsv
store the_dt into 'DT.tsv' using PigStorage('\t');

