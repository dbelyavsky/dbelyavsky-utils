package com.adsafe.scoring.impression.scores;

public interface InvalidTrafficScoreCodesNEW {
    public static final String
            NA = "",
            GENERAL_INVALID_TRAFFIC = "givt",
            NON_HUMAN_TRAFFIC = "nht",
            NHT_SITTING_DUCKS_BOTS = "nht1",
            NHT_STANDARD_BOTS = "nht2",
            NHT_VOLUNTEER_BOTS = "nht3",
            NHT_PROFILE_BOTS = "nht4",
            NHT_MASKED_BOTS = "nht5",
            NHT_NOMADIC_BOTS = "nht6",
            OTHER_BOTS = "nht7",
            HIDDEN_ADS = "ha",
            HA_PIXEL_STUFFING = "haps",
            HA_AD_STACKING = "haas",
            HA_TRUNCATED_ADS = "hatr",
            HA_OFF_PAGE_ADS = "haop",
            HA_CLEAR_ADS = "hacl",
            DOMAIN_SPOOFING = "ds",
            DS_MISDECLARED_DOMAIN = "dsm",
            DS_CROSS_SITE_EMBEDDING = "dse",
            LOCATION_SPOOFING = "ls",
            LS_GEO_COORDINATES = "lsc",
            LS_PROXY_SERVER = "lsp",
            INCENTIVIZED_BROWSING = "ib",
            AUTO_REFRESHED_ADS = "arf",
            IN_BANNER_VIDEO = "bv",
            AD_REINSERTION = "rei";
}