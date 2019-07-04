package com.adsafe.eventlog.log;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.Collection;
import java.util.Map;

/**
* Sad Evidence field is a lis of coma-separated model groups, each group is in a form of "[type]:[model.list]"<br>
* - the model.list is a "."(period)-separated lis of [modelEvidence] strings: <br>
* -- where "model" is encoded as a string of one or more lower case letters ([a-z])<br>
* -- and the "evidence" is any upper case letter or a number ([A-Z0-9])
*/
public class SadEvidence {
	private ListMultimap<String, String> data = ArrayListMultimap.create();

	public SadEvidence(String sadEvidenceString) {
		if (null != sadEvidenceString && ! sadEvidenceString.isEmpty()) {
			Map<String, String> models = Splitter
			.on(",")
			.trimResults(CharMatcher.anyOf("{, }"))
			.omitEmptyStrings()
			.withKeyValueSeparator(":")
			.split(sadEvidenceString);
			for (String modelType : models.keySet()) {
				data.putAll(modelType, Splitter.on(".").splitToList(models.get(modelType)));
			}
		}
	}

	public boolean containsModel(String modelType, String modelKey) {
		return data.containsEntry(modelType, modelKey);
	}

	public boolean containsModel(String modelKey) {
		return data.containsValue(modelKey);
	}

	public Collection<String> getModels() {
		return data.values();
	}

	public static void main(String[] args) {
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

		long ITERATIONS = 1000000;
		long start, finish;
		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		start = threadMXBean.getCurrentThreadCpuTime();
		for (int i = 0; i < ITERATIONS; i++) {
			for (String[] sample : sadEvidenceSamples) {
				new SadEvidence(sample[0]);
			}
		}
		finish = threadMXBean.getCurrentThreadCpuTime();
		System.out.println("CPU time with new: " + (finish - start));

		start = threadMXBean.getCurrentThreadCpuTime();
		for (int i = 0; i < ITERATIONS; i++) {
			for (String[] sample : sadEvidenceSamples) {
				new SadEvidenceOLD(sample[0]);
			}
		}
		finish = threadMXBean.getCurrentThreadCpuTime();
		System.out.println("CPU time with old: " + (finish - start));
	}
}

class SadEvidenceOLD {
	private ListMultimap<String, String> data = ArrayListMultimap.create();

	public SadEvidenceOLD(String sadEvidenceString) {
		if (null != sadEvidenceString) {
			for (String modelsByType : sadEvidenceString.split("[{, }]")) {
				if (modelsByType.contains(":")) {
					String[] typeVsResults = modelsByType.split("[:.]");
					data.putAll(typeVsResults[0], Arrays.asList(typeVsResults).subList(1, typeVsResults.length));
				}
			}
		}
	}

	public boolean containsModel(String modelType, String modelKey) {
		return data.containsEntry(modelType, modelKey);
	}

	public boolean containsModel(String modelKey) {
		return data.containsValue(modelKey);
	}

	public Collection<String> getModels() {
		return data.values();
	}
}
