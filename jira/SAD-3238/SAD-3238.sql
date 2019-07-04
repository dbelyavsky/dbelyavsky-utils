use dashboard;

select browserType
	, browserVersion
	, osType
	, osVersion
	, substring_index(substring_index(no,'||',9), '||', -1) as product
from impressions
where mvn like '%,no=7%'
group by browserType, browserVersion, osType, osVersion, product;

select userAgent
	, browserType
	, browserVersion
	, osType
	, osVersion
	, substring_index(substring_index(no,'||',9), '||', -1) as product
from impressions
where mvn like '%,no=7%' and ((browserType = 'Opera' and browserVersion < 15) OR (browserType = 'IE' and browserVersion < 11))
group by userAgent, browserType, browserVersion, osType, osVersion, product;


select no
	, browserType
	, browserVersion
	, osType
	, osVersion
	, substring_index(substring_index(no,'||',9), '||', -1) as product
from impressions
where mvn like '%,no=7%' and ((browserType = 'Opera' and browserVersion < 15) OR (browserType = 'IE' and browserVersion < 11))
group by no, browserType, browserVersion, osType, osVersion, product;
