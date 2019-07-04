-- fraud@db01

SELECT * -- `id`,`location`
FROM `blacklist_snapshot`
ORDER BY `date` DESC
LIMIT 1
;
-- -------------------------------------------------------------------------------------
-- firewall@s-etldb01
select snapshot_id, incremental_fl, count(*)
from SAD_BLOCKLIST
group by snapshot_id, incremental_fl
;

-- incremental update query
SELECT new.KEY, new.TYPE, new.MODEL, new.EVIDENCE
FROM SAD_BLOCKLIST_NEW new LEFT JOIN SAD_BLOCKLIST old
	ON (new.KEY = old.KEY and new.MODEL = old.MODEL and new.TYPE = old.TYPE)
WHERE old.`KEY` IS NULL
;
