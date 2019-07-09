## How the results were generated

1. The **Invalid Record** counts were extracted from the EventAgg logs (on p-etl06) using the following command:
>    grep -P -B 30 'bad records number' /home/thresher/log/event-agg_201905.log \
    | sed 's/\[.*\]//g' \
    | grep -E 'updateLastHitDate_|bad records number [0-9]+|^--$' \
    | tr -d '\n' \
    | sed 's/--/\n/g' \
    | sed -E 's/.* updateLastHitDate_([0-9]+) .* bad records number ([0-9]+) .*/\1,\2/g' \
    | awk '{sum += $17} END {print sum}'

2. All other counts -- **Total**, **Suspicous(SIVT)** and **General Invalid** (i.e. **GIVT** based on **IAB Browser** and **Bot** as well as **ABF** lists ), as well as by the platform breakups of all categories -- were extracted from impression logs using `spark scala` script **[SAD-5865-hourly.scala](./SAD-5865-hourly.scala)**, because the data is large this had to be done in hourly batches.

 - *NOTE: the above script runs for a very long time(4+ days) and can occassionally fail due to various reasons, such as out of memory in spark shell, or running into the midnight cutoff on the Saturn cluster.  It's written so that it can be restarted and pick up from where it left off, but on occassion it can `loose an hour` of calculation.  To verify, run the command bellow, if any missing hours are identified: remove that `hour` output dir, and re-start the script*: 
```
for d in `seq -w 1 31`; do 
    for h in `seq -w 0 23` ; do 
        [[ `hdfs dfs -count jira/SAD-5865/hourly/2019/05/$d/$h | awk '{print $2}'` = "0" ]] && \
            echo "the directory jira/SAD-5865/hourly/2019/05/$d/$h is empty"
    done
done
```

3. the results from above where then dumped into a single file to speed up the next step.  Once again we're scripting this to do an hourly scan because there are a lot of files (mostly empty) in the output of (2) and their sheer number causes spark to run out of memory: 
 ```
for d in `seq -w 1 31`; do 
    for h in `seq -w 0 23` ; do 
        echo "Dumping May $d $h:00"
        hdfs dfs -text jira/SAD-5865/hourly/2019/05/$d/$h/* >> SAD-5865-hourly.json
    done
done
```
 - _NOTE:The `json` from (3) was also converted to a **[SAD-5865-hourly.csv](./SAD-5865-hourly.csv)** using **[SAD-5865-json2csv.py](./SAD-5865-json2csv.py)**.  This is included in the final results spreadsheet as a separate tab._

4. The hourly results as a single file (**[SAD-5865-hourly.json](./SAD-5865-hourly.json)**) from (3) were uploaded to hdfs and combined in with final `spark scala` script **[SAD-5865-total.scala](./SAD-5865-total.scala)** and stored in **[SAD-5865-total.csv](./SAD-5865-total.csv)**

5. finally everything is compiled in **[SAD-5865.xlsx](./SAD-5865.xlsx)**
