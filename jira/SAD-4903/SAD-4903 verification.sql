select prod.*, branch.*
from
    (
        select lookupid
            , sum(regexp_replace(impressionscores[' iv1'], '}$', '')) as iv1
            , sum(regexp_replace(impressionscores[' sus'], '}$', '')) as sus
            , sum(regexp_replace(impressionscores[' hasus'], '}$', '')) as hasus
            , sum(regexp_replace(fraudscores[' sivt'], '}$', '')) as sivt
            , sum(regexp_replace(fraudscores[' ha'], '}$', '')) as ha
            , sum(givt) as givt
        from qlog_prod
        where lookupid not in ('0','null','-1')
        group by lookupid
    ) as prod,
    (
        select lookupid
            , sum(regexp_replace(impressionscores[' iv1'], '}$', '')) as iv1
            , sum(regexp_replace(impressionscores[' sus'], '}$', '')) as sus
            , sum(regexp_replace(impressionscores[' hasus'], '}$', '')) as hasus
            , sum(regexp_replace(fraudscores[' sivt'], '}$', '')) as sivt
            , sum(regexp_replace(fraudscores[' ha'], '}$', '')) as ha
            , sum(givt) as givt
        from qlog_branch
        where lookupid not in ('0','null','-1')
        group by lookupid
    ) as branch
where prod.lookupid = branch.lookupid
    and (
        prod.iv1 != branch.iv1
        or prod.sus != branch.sus
        or prod.hasus != branch.hasus
        or prod.sivt != branch.sivt
        or prod.ha != branch.ha
        or prod.sus != branch.sivt
        or prod.givt != branch.givt
        )
