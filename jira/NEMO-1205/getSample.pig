qlog = load '/user/etlstage/event/2017/12/09/*/00/*' using PigStorage('\t');

qlog_922805 = filter qlog by $17 == '922805';

rmf qlog_922805.20171209
store qlog_922805 into 'qlog_922805.20171209' using PigStorage('\t');

