qlog_before = load 'quality/logs/2017/05/01/*/impressions/*' using PigStorage('\t');
qlog_after = load 'quality/logs/2017/05/02/*/impressions/*' using PigStorage('\t');

before_HA_group = GROUP qlog_before BY REGEX_EXTRACT($50, '(hasus=[0-9]+)', 1);
after_HA_group = GROUP qlog_after BY REGEX_EXTRACT($50, '(hasus=[0-9]+)', 1);

before_counted = FOREACH before_HA_group GENERATE group, COUNT(qlog_before);
after_counted = FOREACH after_HA_group GENERATE group, COUNT(qlog_after);

together = UNION before_counted, after_counted;

dump together;

/**************** RESULTS ******************************************************
(hasus=0,97607352)
(hasus=1,205)
(hasus=0,103626325)
(hasus=1,113)

before: (205 / 97607352) * 100 = 0.00021002516285863386
after:  (113 / 103626325) * 100 = 0.00010904565032099711
*******************************************************************************/
