package com.adsafe.scoring.impression;

import com.adsafe.eventlog.log.QualityLogRecord;
import com.adsafe.eventlog.log.QualityLogRecordBuilder;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * Created by dbelyavsky on 4/19/17.
 */
public class HiddenAdScorrerPerformanceTest {
    final static int ITERATIONS = 1000000;
    final static String[] data = {
            "[{sca:[{avgrn1:\"0\",dfp:{df:\"2\",sz:\"1800.1000\",dom:\"img\"},ha1:{res1:\"1\",ps:\"1\",ts:\"1491581677069\",psfr:\"1\"}}],rt:[\"1\",\"1\"],asId:\"9bd1871e-9f37-38b9-1bb8-fb3c6db9fb61\",self:1}]",
            "[{sca:[{avgrn1:\"0\",dfp:{df:\"2\",sz:\"1800.1000\",dom:\"img\"},ha1:{res1:\"0\",ps:\"1\",ts:\"1491581844828\",psfr:\"1\"}}],rt:[\"1\",\"1\"],asId:\"3d560ee3-ca6a-dc25-f7bb-69b84f4abd24\",self:0}]",
            "[{sca:[{avgrn1:\"0\",dfp:{df:\"2\",sz:\"728.90\",dom:\"img\"},ha1:{res1:\"1\",ps:\"1\",ts:\"1492531366379\",psfr:\"1\"}},{ha1:{res1:\"0\",ps:\"0\",ts:\"1492531367138\",psfr:\"na\"}}],im:[{pBlk:\"69\"},{pWait:\"34\"},{pLoad:\"1150\"}],rt:[\"1\",\"1\",\"1\",\"1\",\"1\",\"1\",\"1\",\"1\"],asId:\"8fa97f2a-5db1-2505-8e3d-dd8fcb1533b1\",self:1},{sca:[{avgrn1:\"0\",dfp:{df:\"2\",sz:\"728.90\",dom:\"img\"}}],im:[{pWait:\"70\",pLoad:\"502\"},{pBlk:\"95\"}],rt:[\"1\",\"1\",\"1\",\"1\",\"1\",\"1\"],asId:\"1c727e98-bba8-c147-85f9-177c51874109\"},{sca:[{ha1:{res1:\"0\",ps:\"0\",ts:\"1492531367139\",psfr:\"na\"}},{avgrn1:\"0\",dfp:{df:\"2\",sz:\"728.90\",dom:\"img\"},ha1:{res1:\"1\",ps:\"1\",ts:\"1492531366787\",psfr:\"1\"}}],im:[{pWait:\"56\"},{pLoad:\"775\"},{pBlk:\"66\"}],rt:[\"1\",\"1\",\"1\",\"1\",\"1\",\"1\",\"1\"],asId:\"1e8a79bc-5870-bf06-5aef-f2df779b474c\"},{sca:[{avgrn1:\"0\",dfp:{df:\"2\",sz:\"728.90\",dom:\"img\"},ha1:{res1:\"1\",ps:\"1\",ts:\"1492531366574\",psfr:\"1\"}},{ha1:{res1:\"0\",ps:\"0\",ts:\"1492531367136\",psfr:\"na\"}}],im:[{pLoad:\"947\"},{pBlk:\"65\"},{pWait:\"54\"}],rt:[\"1\",\"1\",\"1\",\"1\",\"1\",\"1\",\"1\"],asId:\"25647a1c-db9e-d061-54cf-53483ee721e4\"},{sca:[{avgrn1:\"0\",dfp:{df:\"2\",sz:\"728.90\",dom:\"img\"}}],im:[{pBlk:\"69\"},{pLoad:\"220\"},{pWait:\"15\"}],rt:[\"1\",\"1\",\"1\",\"1\",\"1\"],asId:\"b04fc9d8-bc53-c6dd-59ed-dad15ae2b462\"}]"
    };

    final static QualityLogRecord[] qlr = {
            QualityLogRecordBuilder.aQualityLogRecord().withDtMinimizerValue(data[0]).build(),
            QualityLogRecordBuilder.aQualityLogRecord().withDtMinimizerValue(data[1]).build(),
            QualityLogRecordBuilder.aQualityLogRecord().withDtMinimizerValue(data[2]).build()
    };

    public static void main(String[] args) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        long start, finish;
        start = threadMXBean.getCurrentThreadCpuTime();
        runWithJackson();
        finish = threadMXBean.getCurrentThreadCpuTime();
        System.out.printf("Jackson Run took  %,d cycles\n", (finish - start));

        start = threadMXBean.getCurrentThreadCpuTime();
        runWithGson();
        finish = threadMXBean.getCurrentThreadCpuTime();
        System.out.printf("Gson Run took %,d cycles\n", (finish - start));
        /********************/
        start = threadMXBean.getCurrentThreadCpuTime();
        runWithJackson();
        finish = threadMXBean.getCurrentThreadCpuTime();
        System.out.printf("Jackson Run took  %,d cycles\n", (finish - start));

        start = threadMXBean.getCurrentThreadCpuTime();
        runWithGson();
        finish = threadMXBean.getCurrentThreadCpuTime();
        System.out.printf("Gson Run took %,d cycles\n", (finish - start));
        /*******************/
        start = threadMXBean.getCurrentThreadCpuTime();
        runWithJackson();
        finish = threadMXBean.getCurrentThreadCpuTime();
        System.out.printf("Jackson Run took  %,d cycles\n", (finish - start));

        start = threadMXBean.getCurrentThreadCpuTime();
        runWithGson();
        finish = threadMXBean.getCurrentThreadCpuTime();
        System.out.printf("Gson Run took %,d cycles\n", (finish - start));

    }

    static void runWithGson() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (QualityLogRecord q : qlr) {
                HiddenAdScorer had = new HiddenAdScorer();
                had.computeScores(q);
            }
        }
    }

    static void runWithJackson() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (QualityLogRecord q : qlr) {
                HiddenAdScorerExperimental had = new HiddenAdScorerExperimental();
                had.computeScores(q);
            }
        }
    }
}
