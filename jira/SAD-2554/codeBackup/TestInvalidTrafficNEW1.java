package com.adsafe.scoring.impression;

import com.adsafe.eventlog.log.FirewallLogRecord;
import com.adsafe.eventlog.log.LogRecord;
import com.adsafe.eventlog.log.QualityLogRecord;
import com.adsafe.scoring.impression.util.Scores;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Enclosed;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.adsafe.scoring.impression.scores.InvalidTrafficScoreCodes.NON_HUMAN_TRAFFIC;
import static org.junit.Assert.*;

@RunWith(Enclosed.class)
public class TestInvalidTrafficNEW1 {
    public static class TestInvalidTrafficNonParametrized {
        InvalidTrafficNEW1 ivtScorer = new InvalidTrafficNEW1();

        @Test
        public void shouldNOTComputeScoresWithNoQlog() {
            LogRecord log = new FirewallLogRecord();
            Scores s = ivtScorer.computeScores(log);

            assertTrue(s.getScores().size() == 0);
        }

        @Test
        public void shouldNOTComputeScoresWithoutValidQlogFields() {
            LogRecord log = new QualityLogRecord();
            Scores s = ivtScorer.computeScores(log);
            assertTrue(s.getScores().size() == 0);
        }

        @Test
        public void compute_NON_HUMAN_TRAFFIC() {
            QualityLogRecord qlog = new QualityLogRecord();
            qlog.scores = "{rsa=0}";
            qlog.sadEvidence = "{h:mw, b:spf}";

            Scores s = ivtScorer.computeScores(qlog);
            assertTrue(s.getScores().containsKey(NON_HUMAN_TRAFFIC.getCode()));
            assertEquals(s.getScore(NON_HUMAN_TRAFFIC.getCode()), 1.0f, 0.0f);
        }

        @Test
        public void compute_NON_HUMAN_TRAFFIC_not_set() {
            QualityLogRecord qlog = new QualityLogRecord();
            qlog.scores = "{rsa=100}";
            qlog.sadEvidence = "{h:mw, b:spf}";

            Scores s = ivtScorer.computeScores(qlog);
            assertFalse(s.getScores().containsKey(NON_HUMAN_TRAFFIC.getCode()));
        }
    }
    /**
     * tests specific to bot scores based on sadEvidence data
     */
    @RunWith(Parameterized.class)
    public static class TestInvalidTrafficParametrizedSadEvidenceForBots {
        QualityLogRecord qlog = new QualityLogRecord();
        InvalidTrafficNEW1 ivtScorer = new InvalidTrafficNEW1();

        @Parameterized.Parameter
        public String sadEvidence;
        @Parameterized.Parameter(value = 1)
        public String expectedResult;

        @Parameterized.Parameters
        public static Collection<String[]> data() throws IOException {
            return loadTestData("src/test/resources/com/adsafe/scoring/impression/InvalidTraffic/sadEvidenceBots.tsv");
        }

        @Test
        public void shouldComputeScores() {
            qlog.scores = "{rsa=0}";
            qlog.sadEvidence = sadEvidence;
            Scores scores = ivtScorer.computeScores(qlog);
            assertTrue(validate(scores, expectedResult));
        }
    }

    /**
     * tests for scores based on sadEvidence data
     */
    @RunWith(Parameterized.class)
    public static class TestInvalidTrafficParametrizedSadEvidence {
        QualityLogRecord qlog = new QualityLogRecord();
        InvalidTrafficNEW1 ivtScorer = new InvalidTrafficNEW1();

        @Parameterized.Parameter
        public String sadEvidence;
        @Parameterized.Parameter(value = 1)
        public String expectedResult;

        @Parameterized.Parameters
        public static Collection<String[]> data() throws IOException {
            return loadTestData("src/test/resources/com/adsafe/scoring/impression/InvalidTraffic/sadEvidence.tsv");
        }

        @Test
        public void shouldComputeScores() {
            qlog.sadEvidence = sadEvidence;
            Scores scores = ivtScorer.computeScores(qlog);
            assertTrue(validate(scores, expectedResult));
        }
    }

    /**
     * tests for scores based on dtMinimizer data
     */
    @RunWith(Parameterized.class)
    public static class TestInvalidTrafficParametrizedDtMinimizer {
        QualityLogRecord qlog = new QualityLogRecord();
        InvalidTrafficNEW1 ivtScorer = new InvalidTrafficNEW1();

        @Parameterized.Parameter
        public String dtMinimizer;
        @Parameterized.Parameter(value = 1)
        public String expectedResult;

        @Parameterized.Parameters
        public static Collection<String[]> data() throws IOException {
            return loadTestData("src/test/resources/com/adsafe/scoring/impression/InvalidTraffic/dtMinimizer.tsv");
        }

        @Test
        public void shouldComputeScores() {
            qlog.dtMinimizer = dtMinimizer;
            Scores scores = ivtScorer.computeScores(qlog);
            assertTrue(validate(scores, expectedResult));
        }
    }

    public static Collection<String[]> loadTestData(String testDataFilePath) throws IOException {
        Collection<String[]> retVal = new ArrayList<String[]>();
        BufferedReader in = new BufferedReader(new FileReader(testDataFilePath));
        String line = null;
        while ((line = in.readLine()) != null) {
            retVal.add(line.split("\t"));
        }
        return retVal;
    }

    public static boolean validate(Scores returnedScores, String expectedScores) {
        String[] scoresArray = expectedScores.split(",");
        for(String aScore : scoresArray) {
            String[] nameValue = aScore.split("=");
            if (! returnedScores.hasScore(nameValue[0]) || ! returnedScores.getScore(nameValue[0]).toString().equals(nameValue[1])) {
                return false;
            }
        }
        return true;
    }
}