-- for campaign_id = 39261 and agency_id = 398
-- lookupId = 48752

-- for publisher_id = 1156 and placement_id = 2091236
-- passbackId in (7830682, 7830683, 7830684, 7830750, 7830751)

/user/dbelyavsky/aggdt3_BRANCH_39261/
/user/dbelyavsky/aggdt3_MSTR_39261

grunt> mstr_grp = group mstr by media_type_id;
grunt> branch_grp = group branch by media_type_id;

grunt> mstr_sums = foreach mstr_grp generate flatten(group), SUM(mstr.ov1imps), SUM(mstr.ov2imps), SUM(mstr.imps);
-- (111,0.0,0.0,1.0)
-- (121,187.0,0.0,710.0)
-- (131,3.0,0.0,3.0)
-- (221,6390.0,0.0,42335.0)
grunt> branch_sums = foreach branch_grp generate flatten(group), SUM(branch.ov1imps), SUM(branch.ov2imps), SUM(branch.imps);
-- (111,0.0,0.0,1.0)
-- (121,187.0,0.0,710.0)
-- (131,3.0,0.0,3.0)
-- (221,6391.0,0.0,42335.0)

grunt> events = LOAD_EVENTS('/user/etldev/event/2016112*/*20161120*');
grunt> events_39261 = filter events by lookupId == 48752 and passbackId in (7830682, 7830683, 7830684, 7830750, 7830751);

dt = LOAD_DT('/user/etldev/dt/raw/2016112*/*20161120*');
dt_39261 = filter dt by adv_entity_id == 48752 and pub_entity_id in (7830682, 7830683, 7830684, 7830750, 7830751);

store events_39261 into 'events_39261' using PigStorage('\t');      -- 43049 records

asids_all = foreach events_39261 generate REGEX_EXTRACT(javascriptInfo, ' id=[^,]*', 0);
asids = DISTINCT asids_all;
store asids into 'asids_39261' USING PigStorage('\t');

dt_39261 = JOIN dt BY asid, asids by $0;

store dt_39261 into 'dt_39261' using PigStorage('\t');
-- http://p-hdprm02.303net.pvt:19888/jobhistory/attempts/job_1468336347398_294512/r/FAILED
