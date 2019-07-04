package com.integralads;

import com.integralads.pojo.AggregateNetworkQualityPreResult;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.integralads.builder.AdNetworkRowBuilder.anAdNetwork;
import static com.integralads.builder.OverrideBuilder.anInput;
import static com.integralads.builder.PigTestBuilder.aPigJob;
import static com.integralads.builder.QualityPremartBuilder.aQualityPremart;
import static com.integralads.pojo.AggregateNetworkQualityPreResult.buildAggregateNetworkQualityPreResultfromTupleIterator;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class TestThatTheQualityV2Script extends BasePigUnitTest {


    @Test
    public void canCalculateTheTimeOnPage() throws IOException, ParseException {

        given(anInput().aliasing("ad_network")
                        .providingALogFileContaining(
                                anAdNetwork()
                        ).withSchema("as (id:int, name:chararray, partner_code:chararray)"),

                anInput().aliasing("quality")
                        .providingALogFileContaining(
                                aQualityPremart().withTimeOnPage(2),
                                aQualityPremart().withTimeOnPage(8)
                        ).withSchemaFromLoadFile("LOAD_QUALITY_PREMART")

        );

        when(aPigJob("bin/pig/agg_network_quality_V2.pig"));

        List<AggregateNetworkQualityPreResult> aggregateNetworkQualityPreResult = buildAggregateNetworkQualityPreResultfromTupleIterator(pigTest.getAlias("agg_network_quality_pre"));

        double timeOnPage = aggregateNetworkQualityPreResult.get(0).timeOnPage;

        assertThat("The time on page in the aggregate network quality pre result ", timeOnPage , is(equalTo(4d))) ;
    }

}
