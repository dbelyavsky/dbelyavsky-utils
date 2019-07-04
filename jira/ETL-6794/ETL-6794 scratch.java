import static com.integralads.poseidon.logrecordfields.domain.TSFieldName.FW_MACROS;

public final class ReportConfigFieldProjectionStrategy {

	private static final String FW_MACRO_DELIMITER = "&";
	private static final String FW_MACRO_PREFFIX = "thru_";

	/* ..... */

	private void spinUpCMReportFromFWMacros(PDFields<TSFieldName> fields, TrafficScopeReport reportConf) {
		String firewallMacrosString;
		if (TSReportType.FW == reportConf.getReportType()
				&& fields.getField(FW_MACROS).isPresent()
				&& null != (firewallMacrosString = fields.getField(FW_MACROS).toString())) {
			PDFields<TSFieldName> cmFields = new PDFields.PDFieldsBuilder<TSFieldName>().fromFields(fields).build();
			PDField<TSFieldName> aField;
			for (String macroField : firewallMacrosString.split(FW_MACRO_DELIMITER)) {
				String[] tagValue = macroField.split("=");
				if (tagValue.length == 2 && tagValue[0].startsWith(FW_MACRO_PREFFIX)) {
					aField = PDField.fromNullableValue(TSFieldName.newInstance(
							tagValue[0].substring(4))
							, tagValue[1]);
					cmFields.putField(aField);
				}
			}
		}
	}

	/* ......... */
}

public class TestThatTheTraficScopeMapper {
	static final String[] qlogStrRecordsWithFWMacro = {
        "8.75\t2017-11-16 12:02:57\tja2q5089\t0\t97.67.203.3\tapp25dal\tFirewall\t-1\t1\thttps://www.ebay.com\t/myb/Summary?MyEbay\t&gbh=1\t0\t{iviab=35, pub_dwdv=15, grm_dwdv_728x90=25, mrc_uwdv_320x50=35, ugc=1000, ugb=1000, mrc_mwdv_300x250=5, ugd=1000, ugf=1000, mrc_dwdv_160x600=5, vqm_vins=3, niv=65, ugm=1000, grm_uwdv_300x250=5, pub_dwdv_728x90=15, pub_uwdv_160x600=5, vio=1000, ugs=1000, ivl_160x600=5, mrc_uwdv_728x90=25, iviab_300x250=5, ugt=1000, iab_tech=450, grm_dwdv=15, zmeh=250, pub_dwdv_300x250=5, iviab_160x600=15, mrc_uwdv=15, ivp_160x600=5, mrc_mwdv=15, zult=1000, gmb=1000, rsa=1000, grm_mwdv_728x90=15, vqm_vaut=0, pol=1000, grm_mwdv_300x250=5, top=25000, arf=1000, viv2=5, zdvr=250, zstb=250, mrc_uwdv_300x250=5, grm_uwdv=15, mrc_mwdv_320x50=35, mrc_dwdv=15, zyhvs=250, vqm_vinb=3, off=1000, ztraf=250, grm_mwdv=5, mrc_dwdv_300x250=5, zibm=250, pub_uwdv_320x50=15, mrc_dwdv_728x90=25, iab_shop=450, mrc_uwdv_160x600=5, mwdv=15, grm_uwdv_320x50=25, pac=3, zdmer=250, drg=1000, alc=1000, iviab_728x90=35, hat=1000, vqm_vcnt=0, pub_uwdv_728x90=15, par=1000, pub_dwdv_160x600=5, iv2=25, visibility=1000, ivp_300x250=5, zenmer=250, iv3=25, zsmg=250, pub_mwdv_320x50=15, pro=1000, zsmj=250, adt=1000, trq=600, pub_uwdv_300x250=5, pub_mwdv_300x250=5, zdpsg=250, iab_business=450, grm_dwdv_300x250=5, grm_dwdv_160x600=15, grm_uwdv_160x600=15, mrc_mwdv_728x90=25, ivl_300x250=15, grm_mwdv_320x50=25, grm_uwdv_728x90=25, ivp_728x90=25, pub_mwdv=5, v_c=0, lang=9, ivl_728x90=25, ivl=15, sam=1000, pub_uwdv=15, ivp=15, dlm=875, zbnb=250, ivt=6500, ivu=25, pub_mwdv_728x90=5, webmail=1000}\t114831\t{adt=651, rsa=401, dlm=651, drg=651, alc=501, hat=651, zpub=251, vio=601, off=651, sam=401}\tpassed\tnull\tnull\tnull\tnull\tnull\tnull\tnull\t\tnone\tUS\tnull\tnull\tSC\t18915207\tnull\t567\t1\trfw\tMozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko\te\t{tt=rjss, rt=1, fif=0, cmps=1, pt=1-5-15, obst=0, fm=qBa45gO+111|112|121|131|132|141|151|16*.114831-18915207|161|17|18|19|1a, dm=11, oid=fe15dea3-caef-11e7-b7d7-382c4ac7ff93, fr=false, wc=-8.-8.1936.1056, sa=1, reas=l, br=i, fv=26.0.0.120, sc=1, uf=0, bv=11, sl=outOfView, gtpl=0, mf=133067020, id=00010397-e445-15ec-66ee-912b2a48f34d, wr=1936.1056, piv=0, dtm=i, sp=0, cb=0, sr=1920.1080, cc=1012.3173.300.250, mn=app34dal, ac=1012.3173.300.250, c=u8bFy1, gm=1, ov=0, em=true, am=i, an=n, es=0, et=68, ct=na, th=0, abv=11, v=17.4.42, idMap=16*, ha=1, pl=TWya}\toth\t1\t8WjrHA1JhP8w\t1\tja2q50ff\t{c:u8bFC6,pingTime:-2,time:320,type:a,sca:{dfp:{df:4,sz:300.250,dom:ins}},env:{sf:0,pom:0},rt:1,cb:0,th:0,es:0,sa:1,sc:1,ha:1,fif:0,gm:1,slTimes:{i:0,o:320,n:0,pp:0,pm:0},slEvents:[{sl:o,t:67,wc:-8.-8.1936.1056,ac:1012.3173.300.250,am:i,cc:1012.3173.300.250,piv:0,obst:0,th:0,reas:l,cmps:1,bkn:{piv:[308~0],as:[308~300.250]}}],slEventCount:1,em:true,fr:false,uf:0,e:,tt:rjss,dtt:0,fm:qBa45gO 111|112|121|131|132|141|151|16*.114831-18915207|161|17|18|19|1a,idMap:16*,pd:TWya.Flash.ocx,sinceFw:250,readyFired:false}\t0\tnull\t0\tnull\t{}\t3\t{atf=0.0, bill=0.0, bs=0.0, btf=1.0, clu=0.0, cm_1=0.0, cm_3=0.0, fiv0=0.0, fiv1=0.0, fiv15=0.0, fiv2=0.0, fiv5=0.0, fivt=0.0, hasus=0.0, iv1=0.0, iv15=0.0, iv2=0.0, iv3=0.0, ivl=0.0, ivp=0.0, ivt=0.0, maxpiv=0.0, otf=0.0, pro=0.0, q=0.0, q_=0.0, scasus=0.0, srbi=0.009938471, srei=0.0, sris=0.0, srpb=0.0, sus=0.0}\t{api=0.0, bsmp=0.0, ca=1.0, corrupt=0.0, fin=0.0, fin_raw=0.0, fvndw=0.0, grpw=0.0, imp=1.0, ini=1.0, ini_raw=1.0, q=1.0, smp=1.0, smp_raw=1.0, spf=1.0, sprt_env=1.0, sus=1.0, vmeas=1.0, vsmp=0.0, vtrust=1.0, vvsmp=0.0}\t{dterror=00010397-e445-15ec-66ee-912b2a48f34d, vv=novv}\tnull\tnull\tnull\tnull\t{}\tnull\tthru_anId=5002&thru_advId=5uro8q3uo&thru_chanId=&thru_placementId=ua5sp4k4&thru_pubId=&thru_bidurl=&thru_uId=00000000-0000-0000-0000-000000000000&thru_impId=00000000-0000-0000-0000-000000000000&thru_planId=noop\tnull\tnull\t0\tnull\tnull\t0\tnull\t{c:u8bFGh,pingTime:-10,time:579,type:s,mvn:ZnNjPTcsc2Q9Mixubz02LGFzcD0x,fsc:17.4.27v2222222022222000002000200202000000222200000220000000000000000000002220000002000002000000000000000000000002220220200000000000020000000000000000000000002000000000000000000000000222002220002020222220000244400000000423000440000000000002002200000000000000000000002000200200002000000000020002002022000000022000000020202222200000002000000000022200200000000020220200000000000002000200000222000000000000000022220022222000220222000000020200000000000000020000000020000200,sd:MTcuNC4yN3YxOTIwfHwxMDgwfHwxOTIwfHwxMDQwfHwyNHx8MjR8fG58fDE2Lzl8fDE2Lzl8fDB8fDF8fDF8fDB8fDF8fDA-,no:MTcuNC4yN3ZNb3ppbGxhLzUuMCAoV2luZG93cyBOVCA2LjM7IFdPVzY0OyBUcmlkZW50LzcuMDsgLk5FVDQuMEU7IC5ORVQ0LjBDOyAuTkVUIENMUiAzLjUuMzA3Mjk7IC5ORVQgQ0xSIDIuMC41MDcyNzsgLk5FVCBDTFIgMy4wLjMwNzI5OyBydjoxMS4wKSBsaWtlIEdlY2tvfHwxfHwxfHxXaW4zMnx8MHx8MHx8R2Vja298fG58fG58fG58fE5ldHNjYXBlfHxNb3ppbGxhfHxufHwxMXx8MTF8fDMwMA--,asp:1510851777480||c7ad50d3170e91e8529cbe0a6d9c4130||85f4a3794c23ac484bda7e8c96feef11||d6a578c3cc54b6ca4bb744272520a4c8||ca57e87b40cae7e4817b121bd8b2b32a||f31d946ce2bacf92dd34530885c6ef9d||892dc592e58d82d9364e6e407379fd8f||6850622c35d56d05f4dccf138b65dbf2||1510613775}\t{}\t{}\t97.67.203.3\t{cookieDT=00010397-e445-15ec-66ee-912b2a48f34d, dt:a[-2]=00010397-e445-15ec-66ee-912b2a48f34d, dt:s[-10]=00010397-e445-15ec-66ee-912b2a48f34d}\tnull\tnull\tnull\tnull\tnull\t{}\tnull\tnull\t0\ti\t{\"ost\":\"Windows\",\"osv\":\"null\",\"rv\":\"Error\",\"bt\":\"ie\",\"bv\":\"11\",\"pbt\":\"IE\",\"r\":0,\"rules\":[{\"v\":\"7\",\"n\":1,\"r\":\"2\",\"e\":\"java.lang.Exception: 2, v: 7 of Not Found\",\"t\":0},{\"v\":\"2\",\"n\":7,\"r\":\"2\",\"e\":\"14\",\"t\":0},{\"v\":\"2\",\"n\":9,\"r\":\"2\",\"e\":\"18\",\"t\":0},{\"v\":\"7\",\"n\":14,\"r\":\"2\",\"e\":\"java.lang.Exception: 2, v: 7 of Not Found\",\"t\":0},{\"v\":\"2\",\"n\":15,\"r\":\"2\",\"e\":\"22\",\"t\":0},{\"v\":\"6\",\"n\":16,\"r\":\"2\",\"e\":\"22\",\"t\":0},{\"v\":\"2\",\"n\":8,\"r\":\"2\",\"e\":\"19\",\"x\":1,\"t\":0},{\"v\":\"2\",\"n\":17,\"r\":\"1\",\"i\":[\"1:1:0\"],\"x\":1,\"t\":0},{\"v\":\"7\",\"n\":19,\"r\":\"2\",\"e\":\"java.lang.Exception: 2, v: 7 of Not Found\",\"x\":1,\"t\":0},{\"v\":\"7\",\"n\":20,\"r\":\"2\",\"e\":\"java.lang.Exception: 2, v: 7 of Not Found\",\"x\":1,\"t\":0},{\"v\":\"1\",\"n\":21,\"r\":\"0\",\"x\":1,\"t\":0},{\"v\":\"1\",\"n\":22,\"r\":\"1\",\"i\":[\"0:0:1\"],\"x\":1,\"t\":0,\"ix\":\"39af37d86a703ef1ca43a29654a6a561b2eaccc1\"}],\"m\":[\"7v1\",\"2v2\",\"2v3\",\"2v4\",\"2v5\",\"2v6\",\"2v7\",\"2v9\",\"6v10\",\"6v11\",\"6v12\",\"6v13\",\"7v14\",\"2v15\",\"6v16\",\"2v8\",\"2v17\",\"7v19\",\"7v20\",\"1v21\",\"1v22\"],\"h\":\"https://www.ebay.com\",\"fm\":1}\t0\t18915207\tpassed\t[{sca:[{dfp:{df:\"4\",sz:\"300.250\",dom:\"ins\"}}],rt:[\"1\"],asId:\"00010397-e445-15ec-66ee-912b2a48f34d\",self:1}]\tnull\tnull\tnull\tnull\tnull\t0\tnull\tnull\t{}\t1\tniv\tnull"
    };

    @Test
    public void willProduceCMReportFromFWMacro() throws IOException, URISyntaxException, InterruptedException {
        final File trafficScoreFile = temporaryFolder.newFile("reportConfigFile");

        for (String qlogStr : qlogStrRecordsWithFWMacro) {
            QualityLogRecord qualityLogRecord = new QualityLogRecord(qlogStr);

            given( aTrafficScopeMap()
                    .withCacheFile(trafficScoreFile.getPath())
                    .withConfiguration("map.input.file", "quality/logs/2016/04/10/23/impressions/impressionTest.gz")
                    .withInput(someInputDataForTheMapper()
                            .including(qualityLogRecord)
                    )
            )
                    .when(theMapperExecutes())
                    .then(theOutputFromTheMapper()
                            .atIndex(0)
                            .willHaveValue(new Text("-1.0\u00011.0\u00011.0\u00011.0")
                            )
                    );
        }
    }
}
/****************************************/
public static final HashMap macro2TSFieldName = new HashMap();
static {
	macro2TSFieldName.put("thru_pubId", );
	macro2TSFieldName.put("thru_campId", );
	macro2TSFieldName.put("thru_advId", );
	macro2TSFieldName.put("thru_chanId", );
	macro2TSFieldName.put("thru_placementId", );
	macro2TSFieldName.put("thru_planId", );
	macro2TSFieldName.put("thru_uId", );
	macro2TSFieldName.put("thru_extImpId", );
	macro2TSFieldName.put("thru_extBidUrl", );
	macro2TSFieldName.put("thru_extBidPrice", );
	macro2TSFieldName.put("thru_pubOrder", );
	macro2TSFieldName.put("thru_pubCreative", );
	macro2TSFieldName.put("thru_custom", );
	macro2TSFieldName.put("thru_custom2", );
	macro2TSFieldName.put("thru_custom3", );
}
