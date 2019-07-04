select sum(case when platform = 'Desktop' then imps else 0 end) as 'Desktop Total'
	, sum(case when platform = 'Mobile Web' then imps else 0 end) as 'Mobile Web Total'
	, sum(case when platform = 'Mobile In-App' then imps else 0 end) as 'Mobile In-App Total'
	, sum(case when platform = 'Desktop' then givt else 0 end) as 'Desktop GIVT'
	, sum(case when platform = 'Mobile Web' then givt else 0 end) as 'Mobile Web GIVT'
	, sum(case when platform = 'Mobile In-App' then givt else 0 end) as 'Mobile In-App GIVT'
	, sum(case when platform = 'Desktop' then sivt else 0 end) as 'Desktop SIVT'
	, sum(case when platform = 'Mobile Web' then sivt else 0 end) as 'Mobile Web SIVT'
	, sum(case when platform = 'Mobile In-App' then sivt else 0 end) as 'Mobile In-App SIVT'
from (
	select (case
			when media_type_id in (221,222,231,232) then 'Mobile In-App'
			when media_type_id in (121,122,131,132) then 'Mobile Web'
			else 'Desktop' end
		) as platform
		, sum(imps) imps
		, sum(sivt_imps) sivt
		, sum(givt_imps) givt
	from AGG_AGENCY_CUSTOM_REPORT
	where hit_date between '2018-05-01' and '2018-05-31'
	group by platform
	union
	select (case
			when media_type_id in (221,222,231,232) then 'Mobile In-App'
			when media_type_id in (121,122,131,132) then 'Mobile Web'
			else 'Desktop' end
		) as platform
		, sum(imps) imps
		, sum(suspicious_imps) sivt
		, sum(general_invalid_imps) givt
	from AGG_NETWORK_QUALITY_V3
	where hit_date between '2018-05-01' and '2018-05-31'
	group by platform
	) data
;
/*

Desktop Total	Mobile Web Total	Mobile In-App Total	Desktop GIVT	Mobile Web GIVT	Mobile In-App GIVT	Desktop SIVT	Mobile Web SIVT	Mobile In-App SIVT
111169341005	85113694786	17658765960	246529475	26888827	1669068	2302675330	837906709	2937845

*/
