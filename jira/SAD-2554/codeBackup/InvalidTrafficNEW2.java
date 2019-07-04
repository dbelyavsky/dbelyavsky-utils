package com.adsafe.scoring.impression;

import com.adsafe.eventlog.log.LogRecord;
import com.adsafe.eventlog.log.QualityLogRecord;
import com.adsafe.eventlog.log.SadEvidenceNEW;
import com.adsafe.scoring.impression.util.Scores;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.adsafe.scoring.impression.scores.InvalidTrafficScoreCodes.*;

/***
 * This class creates InvalidTraffic score field "ivtScores"
 * https://confluence.integralads.com/display/TRAQ/Invalid+Traffic+Metrics
 */
public class InvalidTrafficNEW2 extends Scorer {
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
                SadEvidenceNEW sadEvidence = new SadEvidenceNEW(qlog.sadEvidence);
                ivtScores.addScore(INCENTIVIZED_BROWSING.getCode(), isIncentivizedBrowsing(sadEvidence) ? 1.0f : 0.0f);
                setLocationSpoofing(ivtScores, sadEvidence);
                setBots(ivtScores, qlog.scores, sadEvidence);
            }

            if (null != qlog.dtMinimizer) {
                setHiddenAds(ivtScores, qlog.dtMinimizer);
            }
        }

        return ivtScores;
    }

    /**
     * scope: all impressions
     *
     * @param sadEvidence
     * @return true if sadEvidence field contains a model code of the form d[a-z]
     */
    private boolean isProxyServer(SadEvidenceNEW sadEvidence) {
        for (String model : sadEvidence.getModels()) {
            if (null != model && proxyServerPattern.matcher(model).matches())
                return true;
        }
        return false;
    }

    /**
     * this is a placeholder for Latitude/longitude(GEO) coordinates "lsc" score rule
     * @return
     */
    private boolean isGeoCoordinates() { return false; }

    /**
     * scope: all impressions
     *
     * @param sadEvidence
     * @return true if sadEvidence field contains model code "ib"
     */
    private boolean isIncentivizedBrowsing(SadEvidenceNEW sadEvidence) {
        for (String model : sadEvidence.getModels()) {
            if (null != model && incentivizedBrowsingPattern.matcher(model).matches())
                return true;
        }
        return false;
    }

    /**
     * Evaluate all <b>ha*</b> (Hidden Ad) scores and if at least one of haps, haas, hatr, haop, or hacl is defined<br>
     * set the parent "ha" to 1.0 if and only if one of the "ha*" scores have been set and is 1.0
     *
     * @param scores
     */
    private void setHiddenAds(Scores scores, String dtMinimizerString) {
        setPixelStuffing(scores, dtMinimizerString);
        //setAdStacking();
        //setTruncatedAds();
        //setOffPageAds();
        //setClearAds();
    }

    /**
     * scope: dtMinimizer field contains ha1 field (with either res1=1 or res1=0)
     * will add "haps" score with a value 1.0 if dtMinimizer field contains ha1:{res1:1}
     *
     * @param scores
     * @param dtMinimizer
     */
    private void setPixelStuffing(Scores scores, String dtMinimizer) {
        if (null != dtMinimizer) {
            Matcher m = pixelStuffingPattern.matcher(dtMinimizer);
            while (m.find()) {
                if (m.group(1).equals("1")) {
                    scores.addScore(HA_PIXEL_STUFFING.getCode(), 1.0f);
                    scores.addScore(HIDDEN_ADS.getCode(), 1.0f);
                    return;
                }
            }
            if (dtMinimizer.indexOf("ha1:") >= 0)
                scores.addScore(HA_PIXEL_STUFFING.getCode(), 0.0f);
        }
    }

    private void setBots(Scores ivtScores, String scores, SadEvidenceNEW sadEvidence) {
        if (null != scores) {
            if (scoresRSAPattern.matcher(scores).find()) {
                ivtScores.addScore(NON_HUMAN_TRAFFIC.getCode(), 1.0f);
                int modelEvidence = 0, prevModelEvidence = 0;
                for (String modelKey : sadEvidence.getModels()) {
                    // find all bot ("t\d+") models
                    // full sadEvidence value sample: "{u:e.t4.m56.sC.ea.t4.m56.sC.ba82.bk52.n0.566, b:spf}"
                    // here we're only looking for "t\d+" models
                    //     -- where "t" indicates a bot detection "model",
                    //     -- and the "#" is the "evidence" - i.e. the type of model used
                    // the "model+evidence" can repeate, but will usually be the same
                    // if two different bot models have occured : default to OTHER_BOTS(7)
                    if (botModelPattern.matcher(modelKey).matches()) {
                        modelEvidence = Integer.parseInt(modelKey.substring(1));

                        if (modelEvidence > 0 && prevModelEvidence > 0 && prevModelEvidence != modelEvidence) {
                            modelEvidence = 7;
                            break;  // stop looking for more bot models
                        }
                        prevModelEvidence = modelEvidence;
                    }
                }
                if (modelEvidence > 0) {
                    ivtScores.addScore(botModelsIndex[modelEvidence], 1.0f);
                } else {
                    ivtScores.addScore(OTHER_BOTS.getCode(), 1.0f);
                }
            }
        }
    }

    private void setLocationSpoofing(Scores ivtScores, SadEvidenceNEW sadEvidence) {
        if ( isProxyServer(sadEvidence) ) {
            ivtScores.addScore(LS_PROXY_SERVER.getCode(), 1.0f);
            ivtScores.addScore(LOCATION_SPOOFING.getCode(), 1.0f);
        } else if ( isGeoCoordinates() ) {
            ivtScores.addScore(LS_GEO_COORDINATES.getCode(), 1.0f);
            ivtScores.addScore(LOCATION_SPOOFING.getCode(), 1.0f);
        } else {
            ivtScores.addScore(LOCATION_SPOOFING.getCode(), 0.0f);
        }
    }
}