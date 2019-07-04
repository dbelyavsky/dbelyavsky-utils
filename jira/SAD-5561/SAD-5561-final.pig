%default JIRA 'SAD-5561'
%default MONTH '04'

%default FRAUD_INPUT '$JIRA/2019/$MONTH/*/*/part*'
%default OUTPUT '$JIRA/final/$MONTH'

fraud_counts = LOAD '$FRAUD_INPUT' USING PigStorage('\t') AS (
    date,
    hour,
    totalCount,
    totalDesktop,
    totalMobileWeb,
    totalMobileApp,
    sivtTotal,
    givtTotal,
    sivt_desktop,
    sivt_mobileWeb,
    sivt_mobileApp,
    abfBotCnt_desktop,
    abfBotCnt_mobileWeb,
    abfBotCnt_mobileApp,
    iabBrowserCnt_desktop,
    iabBrowserCnt_mobileWeb,
    iabBrowserCnt_mobileApp,
    iabBotCnt_desktop,
    iabBotCnt_mobileWeb,
    iabBotCnt_mobileApp,
    cloudActivityCnt_desktop,
    cloudActivityCnt_mobileWeb,
    cloudActivityCnt_mobileApp,
    testActivityCnt_desktop,
    testActivityCnt_mobileWeb,
    testActivityCnt_mobileApp
);

result = FOREACH (GROUP fraud_counts ALL) GENERATE
    SUM($1.totalCount),
    SUM($1.totalDesktop),
    SUM($1.totalMobileWeb),
    SUM($1.totalMobileApp),
    SUM($1.sivtTotal),
    SUM($1.givtTotal),
    SUM($1.sivt_desktop),
    SUM($1.sivt_mobileWeb),
    SUM($1.sivt_mobileApp),
    SUM($1.abfBotCnt_desktop),
    SUM($1.abfBotCnt_mobileWeb),
    SUM($1.abfBotCnt_mobileApp),
    SUM($1.iabBrowserCnt_desktop),
    SUM($1.iabBrowserCnt_mobileWeb),
    SUM($1.iabBrowserCnt_mobileApp),
    SUM($1.iabBotCnt_desktop),
    SUM($1.iabBotCnt_mobileWeb),
    SUM($1.iabBotCnt_mobileApp),
    SUM($1.cloudActivityCnt_desktop),
    SUM($1.cloudActivityCnt_mobileWeb),
    SUM($1.cloudActivityCnt_mobileApp),
    SUM($1.testActivityCnt_desktop),
    SUM($1.testActivityCnt_mobileWeb),
    SUM($1.testActivityCnt_mobileApp);

RMF $OUTPUT
STORE result INTO '$OUTPUT' USING PigStorage('\t');
