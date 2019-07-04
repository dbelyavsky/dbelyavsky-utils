package com.adsafe.eventlog.log;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static junit.framework.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestSadEvidenceNEW {
    static String[][] sadEvidenceSamples = {
            {"{u:e.t4.m56.sC.ea.t4.m56.sC.ba82.bk52.n0.566, b:spf}", "u", "m56"},
            {"{u:e.t4.m59.ea.t4.m59, h:mw}", "h", "mw"},
            {"{u:e.t4.m70.ea.t4.m69.ba85.bb85.bc85}", null, "m70"},
            {"{h:mw, b:spf}", null, "spf"},
            {"{a:mod1, z:mod1}", null, "mod1"},
            {"{u:e.t4.m70.ea.t4.m69.ba85.bb85.bc85}", null, "bc85"},
            {"", null, null},
            {null, null, null}

    };

    @Parameterized.Parameter
    public String sadEvidenceString;
    @Parameterized.Parameter(value = 1)
    public String containsModelType;
    @Parameterized.Parameter(value = 2)
    public String containsModelKey;

    @Parameterized.Parameters
    public static Collection<String[]> data() throws IOException {
        return Arrays.asList(sadEvidenceSamples);
    }

    @Test
    public void validateSadEvidenceStrings() {
        SadEvidenceNEW se = new SadEvidenceNEW(sadEvidenceString);
        if (containsModelKey == null && containsModelType == null) {
            assertTrue(se.getModels().isEmpty());
            return;
        }
        if (containsModelType == null) assertTrue(se.containsModel(containsModelKey));
        else assertTrue(se.containsModel(containsModelType, containsModelKey));
    }
}