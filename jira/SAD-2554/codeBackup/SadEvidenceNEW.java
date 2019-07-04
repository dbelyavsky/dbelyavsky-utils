package com.adsafe.eventlog.log;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import java.util.*;

/**
 * Sad Evidence field is a lis of coma-separated model groups, each group is in a form of "[type]:[model.list]"<br>
 * - the model.list is a "."(period)-separated lis of [modelEvidence] strings: <br>
 * -- where "model" is encoded as a string of one or more lower case letters ([a-z])<br>
 * -- and the "evidence" is any upper case letter or a number ([A-Z0-9])
 */
public class SadEvidenceNEW {
    private final List<String> allModels = new ArrayList<String>();
    private final Map<String, List<String>> data = new HashMap<String, List<String>>();

    public SadEvidenceNEW(String sadEvidenceString) {
        if (null != sadEvidenceString && ! sadEvidenceString.isEmpty()) {
            Map<String, String> models = Splitter
                    .on(",")
                    .trimResults(CharMatcher.anyOf("{, }"))
                    .omitEmptyStrings()
                    .withKeyValueSeparator(":")
                    .split(sadEvidenceString);
            for (String modelType : models.keySet()) {
                data.put(modelType, Splitter.on(".").splitToList(models.get(modelType)));
                allModels.addAll(data.get(modelType));
            }
        }
    }

    public boolean containsModel(String modelType, String modelKey) {
        return data.get(modelType).contains(modelKey);
    }

    public boolean containsModel(String modelKey) {
        return allModels.contains(modelKey);
    }

    public Collection<String> getModels() {
        return allModels;
    }
}