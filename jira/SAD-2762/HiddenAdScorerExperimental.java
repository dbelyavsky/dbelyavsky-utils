package com.adsafe.scoring.impression;

import com.adsafe.eventlog.log.LogRecord;
import com.adsafe.eventlog.log.QualityLogRecord;
import com.adsafe.scoring.impression.scores.ImpressionScoreCodes;
import com.adsafe.scoring.impression.util.Scores;
import com.adsafe.util.Platform;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 *   // experimental will compare performance with HiddenAdScorer
 */
public class HiddenAdScorerExperimental extends Scorer {
    private final Pattern MOBILE_APP_PATTERN = Pattern.compile("\\Wmapp=1\\W");
    private final Pattern FRIENDLY_PATTERN = Pattern.compile("\\Wfr=true\\W");
    private final Pattern SELF_ONE_PATTERN = Pattern.compile("\\W\"self\":1\\W");
    private final Pattern DF_PATTERN = Pattern.compile("\\W\"df\":\"2\"\\W");
    private final Pattern HA_VISIBLE_PATTERN = Pattern.compile("\\W\"res1\":\"0\"\\W");
    private final Pattern HA_HIDDEN_PATTERN = Pattern.compile("\\W\"res1\":\"1\"\\W");

    private final String PLATFORM_OTHER = Platform.oth.name();

    @Override
    public Scores computeScores(LogRecord l) {
        Scores scores = new Scores();

        if ( l instanceof QualityLogRecord) {
            QualityLogRecord qlr = (QualityLogRecord) l;
            if (PLATFORM_OTHER.equals(qlr.platform)
                    && FRIENDLY_PATTERN.matcher(qlr.javascriptInfo).find()
                    && ! MOBILE_APP_PATTERN.matcher(qlr.javascriptInfo).find()) {
                JsonFactory jsonFactory = new JsonFactory();
                try {
                    JsonParser jp = jsonFactory.createJsonParser(qlr.dtMinimizer);
                    jp.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
                    ObjectMapper om = new ObjectMapper();
                    JsonNode jn = om.readTree(jp);
                    if (jn.isArray()) {
                        Iterator<JsonNode> dtCallsIterator = jn.iterator();
                        while (dtCallsIterator.hasNext()) {
                            JsonNode dtCall = dtCallsIterator.next();
                            String singleDtCallText = dtCall.toString();
                            if (SELF_ONE_PATTERN.matcher(singleDtCallText).find()) {
                                if (DF_PATTERN.matcher(singleDtCallText).find()
                                        && HA_HIDDEN_PATTERN.matcher(singleDtCallText).find()
                                        && !HA_VISIBLE_PATTERN.matcher(singleDtCallText).find()) {
                                    scores.addScore(ImpressionScoreCodes.HASUS.getCode(), 1.0f);
                                }
                                break;  // only one "SELF" pattern should exist, once found: stop processing this imp
                            }
                        }
                    }
                } catch (IOException e) {
                }
            }
        }

        return scores;
    }
}
