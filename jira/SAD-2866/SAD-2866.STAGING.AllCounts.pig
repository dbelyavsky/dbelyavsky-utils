qlog = load '/user/etlstage/quality/logs/2017/04/21/08/impressions/*' using PigStorage('\t');
qlog_groupped = group qlog by $95;
qlog_groupped_counted = foreach qlog_groupped generate FLATTEN(group), COUNT(qlog);
dump qlog_groupped_counted;

/****************** RESULTS **********************
({ha=1.0, haps=1.0},8)
({nht=1.0, nht3=1.0},1418)
({nht=1.0, nht6=1.0},2699)
({ls=1.0, lsp=1.0, nht=1.0, nht1=1.0, rvi=1.0},288)
({ls=1.0, lsp=1.0, nht=1.0, nht2=1.0, rvi=1.0},93)
({ls=1.0, lsp=1.0, nht=1.0, nht3=1.0, rvi=1.0},44)
({nht=1.0, nht1=1.0},1207)
({nht=1.0, nht4=1.0},7793)
({nht=1.0, nht7=1.0},52275)
({ib=1.0, ls=1.0, lsp=1.0, rvi=1.0},115)
({},5135724)
({ib=1.0, rvi=1.0},12859)
({nht=1.0, nht2=1.0},415)
({nht=1.0, nht5=1.0},20264)
({ls=1.0, lsp=1.0, rvi=1.0},56174)
({ls=1.0, lsp=1.0, nht=1.0, nht4=1.0, rvi=1.0},163)
({ls=1.0, lsp=1.0, nht=1.0, nht5=1.0, rvi=1.0},154)
({ls=1.0, lsp=1.0, nht=1.0, nht6=1.0, rvi=1.0},110)
({ls=1.0, lsp=1.0, nht=1.0, nht7=1.0, rvi=1.0},3270)
**************************************************/
