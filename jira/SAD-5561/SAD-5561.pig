IMPORT '/home/dbelyavsky/src/etlscripts/aggregation_helper/pig/macros/load.macro';

%default HDFS_BASE '/user/thresher/quality/logs'
%default JIRA 'SAD-5561-givt'
%default YEAR '2019'
%default MONTH '04'
%default DAY '01'
%default HOUR '12'

%default INPUT '$HDFS_BASE/$YEAR/$MONTH/$DAY/$HOUR/impressions/*'
%default OUTPUT '$JIRA/$YEAR/$MONTH/$DAY/$HOUR'

qlog = LOAD_JOINED('$INPUT');

identified_metrics = FOREACH qlog GENERATE
    ((platform != 'mob' AND platform != 'tab') ? 1 : 0) as desktop,
    ((platform == 'mob' OR platform == 'tab') AND NOT javascriptInfo MATCHES '.* mapp=1.*' ? 1 : 0) as mobileWeb,
    ((platform == 'mob' OR platform == 'tab') AND javascriptInfo MATCHES '.* mapp=1.*' ? 1 : 0) as mobileApp,
    (fraudScores MATCHES '.* sivt=1.0.*' ? 1 : 0) as sivt,
    (givt == 1 ? 1 : 0) as givt,
    (givt == 1 AND fraudScores MATCHES '.*givta=1.0.*' ? 1 : 0) AS abfBot,
    (givt == 1 AND fraudScores MATCHES '.*givtb=1.0.*' ? 1 : 0) AS iabBrowser,
    (givt == 1 AND fraudScores MATCHES '.*givts=1.0.*' ? 1 : 0) AS iabBot,
    (givt == 1 AND fraudScores MATCHES '.*givtc=1.0.*' ? 1 : 0) AS cloudActivity,
    (givt == 1 AND fraudScores MATCHES '.*givtt=1.0.*' ? 1 : 0) AS testActivity;

metrics_by_platform = FOREACH identified_metrics GENERATE
    desktop as desktop,
    mobileWeb as mobileWeb,
    mobileApp as mobileApp,
    sivt as sivt,
    givt as givt,
    (sivt * desktop) AS sivt_desktop,
    (sivt * mobileWeb) AS sivt_mobileWeb,
    (sivt * mobileApp) AS sivt_mobileApp,
    (abfBot * desktop) AS abfBot_desktop,
    (abfBot * mobileWeb) AS abfBot_mobileWeb,
    (abfBot * mobileApp) AS abfBot_mobileApp,
    (iabBrowser * desktop) AS iabBrowser_desktop,
    (iabBrowser * mobileWeb) AS iabBrowser_mobileWeb,
    (iabBrowser * mobileApp) AS iabBrowser_mobileApp,
    (iabBot * desktop) AS iabBot_desktop,
    (iabBot * mobileWeb) AS iabBot_mobileWeb,
    (iabBot * mobileApp) AS iabBot_mobileApp,
    (cloudActivity * desktop) AS cloudActivity_desktop,
    (cloudActivity * mobileWeb) AS cloudActivity_mobileWeb,
    (cloudActivity * mobileApp) AS cloudActivity_mobileApp,
    (testActivity * desktop) AS testActivity_desktop,
    (testActivity * mobileWeb) AS testActivity_mobileWeb,
    (testActivity * mobileApp) AS testActivity_mobileApp;

result = FOREACH (GROUP metrics_by_platform ALL) GENERATE
    CONCAT('$YEAR', '-', '$MONTH', '-', '$DAY') as date,
    $HOUR as hour,
    COUNT_STAR(metrics_by_platform) as totalCount,
    SUM($1.desktop) AS totalDesktop,
    SUM($1.mobileWeb) AS totalMobileWeb,
    SUM($1.mobileApp) AS totalMobileApp,
    SUM($1.sivt) AS sivtTotal,
    SUM($1.givt) AS givtTotal,
    SUM($1.sivt_desktop) AS sivt_desktop,
    SUM($1.sivt_mobileWeb) AS sivt_mobileWeb,
    SUM($1.sivt_mobileApp) AS sivt_mobileApp,
    SUM($1.abfBot_desktop) AS abfBotCnt_desktop,
    SUM($1.abfBot_mobileWeb) AS abfBotCnt_mobileWeb,
    SUM($1.abfBot_mobileApp) AS abfBotCnt_mobileApp,
    SUM($1.iabBrowser_desktop) AS iabBrowserCnt_desktop,
    SUM($1.iabBrowser_mobileWeb) AS iabBrowserCnt_mobileWeb,
    SUM($1.iabBrowser_mobileApp) AS iabBrowserCnt_mobileApp,
    SUM($1.iabBot_desktop) AS iabBotCnt_desktop,
    SUM($1.iabBot_mobileWeb) AS iabBotCnt_mobileWeb,
    SUM($1.iabBot_mobileApp) AS iabBotCnt_mobileApp,
    SUM($1.cloudActivity_desktop) AS cloudActivityCnt_desktop,
    SUM($1.cloudActivity_mobileWeb) AS cloudActivityCnt_mobileWeb,
    SUM($1.cloudActivity_mobileApp) AS cloudActivityCnt_mobileApp,
    SUM($1.testActivity_desktop) AS testActivityCnt_desktop,
    SUM($1.testActivity_mobileWeb) AS testActivityCnt_mobileWeb,
    SUM($1.testActivity_mobileApp) AS testActivityCnt_mobileApp;

RMF $OUTPUT
STORE result INTO '$OUTPUT' USING PigStorage('\t');

