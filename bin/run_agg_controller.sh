#!/bin/ksh

job_id = get_job_id(qlog_date_path)
copy_eventagg_results(qlog_date_path)
setup_groupm_scale_factors(qlog_date_path)
setup_mart_conf(ib_runtime)
# kick off marting
runIt("hdfs dfs -mkdir -p /user/%s/status/aggcontroller/{todo,in_progress,holding,archive} && "
                "hdfs dfs -touchz /user/%s/status/aggcontroller/todo/%s.%s" %
                            (USER, USER, restatement_date.strftime("%Y%m%d"), job_id))
while not is_marting_done(restatement_date.strftime("%Y%m%d"), job_id):
    fastprint("MARTING IS NOT DONE ... re-running agg controller")
    runIt("/home/{0}/ias_mart/bin/aggregation_controller.sh".format(USER), pty=False)
    time.sleep(300)
fastprint("MARTING IS DONE")
