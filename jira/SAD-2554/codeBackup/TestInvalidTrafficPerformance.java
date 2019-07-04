package com.adsafe.scoring.impression;

import com.adsafe.eventlog.log.*;
import com.adsafe.scoring.impression.util.Scores;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class TestInvalidTrafficPerformance {
    static final long ITERATIONS = 1000000;

    public static void main(String[] args) {
        testSadEvidencePerformance();
        testInvalidTrafficPerformance();
    }

    private static void testInvalidTrafficPerformance() {
        QualityLogRecord log = new QualityLogRecord();
        log.sadEvidence = "{u:e.t4.m56.sC.ea.t4.m56.sC.ba82.bk52.n0.566.ib100, b:spf}";
        log.dtMinimizer = "[{sca:[{dfp:{df:\"4\",sz:\"100.100\",dom:\"iframe\"},ha1:{res1:\"1\",ps:\"1\",ts:\"1489723272129\",psfr:\"1\"}},{ha1:{res1:\"0\",ps:\"0\",ts:\"1489723273816\",psfr:\"na\"}}],rt:[\"1\",\"1\",\"1\",\"1\",\"1\"],asId:\"0025f5ed-da08-ddf7-c635-f7c21def650d\",self:1}]";
        log.scores = "{rsa=0}";

        System.out.println("InvalidTraffic performance test");

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long start, finish;
        double origTime, newTime2;

        Scores scores;

        InvalidTraffic invalidTraffic;
        start = threadMXBean.getCurrentThreadCpuTime();
        for (int i = 0; i < ITERATIONS; i++) {
            invalidTraffic = new InvalidTraffic();
            scores = invalidTraffic.computeScores(log);
        }
        finish = threadMXBean.getCurrentThreadCpuTime();
        origTime = (finish - start);

        InvalidTrafficNEW2 invalidTrafficNEW2;
        start = threadMXBean.getCurrentThreadCpuTime();
        for (int i = 0; i < ITERATIONS; i++) {
            invalidTrafficNEW2 = new InvalidTrafficNEW2();
            scores = invalidTrafficNEW2.computeScores(log);
        }
        finish = threadMXBean.getCurrentThreadCpuTime();
        newTime2 = (finish - start);

        System.out.println("\tCPU Time ORIG:" + origTime + "ms");
        System.out.println("\tCPU Time NEW2:" + newTime2 + "ms, ratio: " + (newTime2 / origTime));
    }

    private static void testSadEvidencePerformance() {

        String[][] sadEvidenceSamples = {
                {"{u:e.t4.m56.sC.ea.t4.m56.sC.ba82.bk52.n0.566, b:spf}", "u", "m56"},
                {"{u:e.t4.m59.ea.t4.m59, h:mw}", "h", "mw"},
                {"{u:e.t4.m70.ea.t4.m69.ba85.bb85.bc85}", null, "m70"},
                {"{h:mw, b:spf}", null, "spf"},
                {"{a:mod1, z:mod1}", null, "mod1"},
                {"{u:e.t4.m70.ea.t4.m69.ba85.bb85.bc85}", null, "bc85"},
                {"", null, null},
                {null, null, null}

        };

        long start, finish;
        double origTime, newTime;

        System.out.println("SadEvidence performance test");

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        start = threadMXBean.getCurrentThreadCpuTime();
        for (int i = 0; i < ITERATIONS; i++) {
            for (String[] sample : sadEvidenceSamples) {
                new SadEvidence(sample[0]);
            }
        }
        finish = threadMXBean.getCurrentThreadCpuTime();
        origTime = (finish - start);

        start = threadMXBean.getCurrentThreadCpuTime();
        for (int i = 0; i < ITERATIONS; i++) {
            for (String[] sample : sadEvidenceSamples) {
                new SadEvidenceNEW(sample[0]);
            }
        }
        finish = threadMXBean.getCurrentThreadCpuTime();
        newTime = (finish - start);

        System.out.println("\tCPU time with OLD: " + origTime + "ms");
        System.out.println("\tCPU time with NEW: " + newTime + "ms, ratio: " + (newTime / origTime));
    }
}