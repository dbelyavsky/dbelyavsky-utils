select jsinfo_pl
    , REMOVE_DUPS(SPLIT(jsinfo_pl, '\\.'))
    , REMOVE_DUPS(ARRAY('AMI3','AMI3','uMJD','OcW9','bIab','RKkB','gMlu'))
from default.qlog_sampled
where parquet_date = 20170120
    and jsinfo_pl in ( 'AMI3.AMI3.uMJD.OcW9.bIab.RKkB.gMlu')
--    and jsinfo_pl in ( 'AMI3.AMI3.uMJD.OcW9.bIab.RKkB.gMlu', 'AMI3.AMI3.OI2w.7PHi.JSY6.rIJv.AITd.ESsP.RKkB')
;

select jsinfo_pl
    , REMOVE_DUPS(split(jsinfo_pl, '\\.'))
from default.qlog_sampled
where parquet_date = 20170120
    and jsinfo_pl IS NOT NULL
    and size(SPLIT(jsinfo_pl, '\\.')) <> size(REMOVE_DUPS(SPLIT(jsinfo_pl, '\\.')))
limit 100;
