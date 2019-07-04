impressions = load '/user/thresher/partner_measured/jas_logs/youtube/2017/11/08/10/00/jas/*.avro' using AvroStorage();
imps = FILTER impressions by suspicious or iabRobot;
group_imps = GROUP imps ALL;
count_imps = FOREACH group_imps GENERATE COUNT(imps);
dump count_imps
-- result: (51997)

my_impressions = load 'partner_measured/jas_logs/youtube/2017/11/08/10/00/jas/*.avro' using AvroStorage();
my_imps = FILTER my_impressions by suspicious or iabRobot;
my_group_imps = GROUP my_imps ALL;
my_count_imps = FOREACH my_group_imps GENERATE COUNT(my_imps);
dump my_count_imps
-- result: (52004)
