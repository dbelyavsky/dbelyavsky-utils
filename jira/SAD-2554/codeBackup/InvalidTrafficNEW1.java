package com.adsafe.scoring.impression;

import com.adsafe.eventlog.log.LogRecord;
import com.adsafe.eventlog.log.QualityLogRecord;
import com.adsafe.eventlog.log.SadEvidence;
import com.adsafe.scoring.impression.util.Scores;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.adsafe.scoring.impression.scores.InvalidTrafficScoreCodes.*;

/***
 * This class creates InvalidTraffic score field "ivtScores"
 * https://confluence.integralads.com/display/TRAQ/Invalid+Traffic+Metrics
 */
public class InvalidTrafficNEW1 extends Scorer {
    // short cut access to bot models using their integer index (avoids long if-else / case)
    private static final String[] botModelsIndex = {
            NA.getCode(),                         // 0 - should never happen
            NHT_SITTING_DUCKS_BOTS.getCode(),     // t1
            NHT_STANDARD_BOTS.getCode(),          // t2
            NHT_VOLUNTEER_BOTS.getCode(),         // t3
            NHT_PROFILE_BOTS.getCode(),           // t4
            NHT_MASKED_BOTS.getCode(),            // t5
            NHT_NOMADIC_BOTS.getCode(),           // t6
            OTHER_BOTS.getCode()                  // t7
    };

    private static final Pattern pixelStuffingPattern = Pattern.compile("\\W*res1:\"(\\d+)\"");
    private static final Pattern scoresRSAPattern = Pattern.compile("\\Wrsa=0\\W");
    private static final Pattern incentivizedBrowsingPattern = Pattern.compile("ib\\d+");
    private static final Pattern proxyServerPattern = Pattern.compile("d[a-z]");
    private static final Pattern botModelPattern = Pattern.compile("t\\d+");

    @Override
    public Scores computeScores(LogRecord log) {
        Scores ivtScores = new Scores();

        if (log instanceof QualityLogRecord) {
            QualityLogRecord qlog = (QualityLogRecord) log;
            if (null != qlog.sadEvidence && ! qlog.sadEvidence.isEmpty()) {
                SadEvidence sadEvidence = new SadEvidence(qlog.sadEvidence);
                ivtScores.addScore(INCENTIVIZED_BROWSING.getCode(), 0.0f);

                boolean nhtDetected = false;
                int modelEvidence = 0, prevModelEvidence = 0;

                if (null != qlog.scores && scoresRSAPattern.matcher(qlog.scores).find()) {
                    ivtScores.addScore(NON_HUMAN_TRAFFIC.getCode(), 1.0f);
                    nhtDetected = true;
                }

                for (String model : sadEvidence.getModels()) {
                    if (null != model) {
                        if (incentivizedBrowsingPattern.matcher(model).matches()) {
                            ivtScores.addScore(INCENTIVIZED_BROWSING.getCode(), 1.0f);
                        }

                        if (proxyServerPattern.matcher(model).matches()) {
                            ivtScores.addScore(LS_PROXY_SERVER.getCode(), 1.0f);
                            ivtScores.addScore(LOCATION_SPOOFING.getCode(), 1.0f);
                        }
                    }

                    if (nhtDetected && botModelPattern.matcher(model).matches()) {
                        modelEvidence = Integer.parseInt(model.substring(1));

                        if (modelEvidence > 0 && prevModelEvidence > 0 && prevModelEvidence != modelEvidence) {
                            modelEvidence = 7;
                        }
                        prevModelEvidence = modelEvidence;
                    }
                }

                if ( ! ivtScores.hasScore(LOCATION_SPOOFING.getCode()) ){
                    ivtScores.addScore(LOCATION_SPOOFING.getCode(), 0.0f);
                }

                if (modelEvidence > 0) {
                    ivtScores.addScore(botModelsIndex[modelEvidence], 1.0f);
                } else {
                    ivtScores.addScore(OTHER_BOTS.getCode(), 1.0f);
                }
            }

            if (null != qlog.dtMinimizer) {
                Matcher m = pixelStuffingPattern.matcher(qlog.dtMinimizer);
                while (m.find()) {
                    if (m.group(1).equals("1")) {
                        ivtScores.addScore(HA_PIXEL_STUFFING.getCode(), 1.0f);
                        ivtScores.addScore(HIDDEN_ADS.getCode(), 1.0f);
                    }
                }
                if ( ! ivtScores.hasScore(HA_PIXEL_STUFFING.getCode()) && qlog.dtMinimizer.indexOf("ha1:") >= 0)
                    ivtScores.addScore(HA_PIXEL_STUFFING.getCode(), 0.0f);
            }
        }

        return ivtScores;
    }
}