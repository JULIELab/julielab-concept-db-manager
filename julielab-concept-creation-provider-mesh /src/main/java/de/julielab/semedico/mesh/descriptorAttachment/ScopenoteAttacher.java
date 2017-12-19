package de.julielab.semedico.mesh.descriptorAttachment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ArrayListMultimap;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.TreeVertex;

public class ScopenoteAttacher extends BaseAttacher {

	public ScopenoteAttacher(Tree data) {
		super(data);
	}

	/*
	 * 
	 */
	public void buildTreeVertexAttachmentForScopenote(String wordlistFileName)
			throws IOException {
		treeVertexAttachments = ArrayListMultimap.create();

		// Firstly, read the existing attachments from tree
		ArrayListMultimap<String, String> scopenoteMap = readScopenoteMap(wordlistFileName);

		// Then, build these directly defined categorizations.
		for (String ui : scopenoteMap.keySet()) {
			List<String> notes = scopenoteMap.get(ui);

			Descriptor descriptor = data.getDescriptorByUi(ui);

			addAttachmentToDescriptor(notes, descriptor);
		}
		afterBuildTreeVertexAttachment();
	}

	/*
	 * read in the scopenotes by modersohn
	 */
	private ArrayListMultimap<String, String> readScopenoteMap(
			String wordlistFileName) throws IOException {
		ArrayListMultimap<String, String> categoryMap = ArrayListMultimap
				.create();

		// First read in the words that should be searches
		Map<String, Integer> wordlist = new HashMap<String, Integer>();

		BufferedReader br = new BufferedReader(new FileReader(wordlistFileName));
		String line;
		while ((line = br.readLine()) != null) {
			// The split isn't really necessary since only the words themselves
			// are required, but it allows to read the words
			// from a TSV files which has additional information (that is
			// ignored here) which can come in handy (you need less files
			// overall).
			wordlist.put(line.split("\t")[0].trim(), 0);
		}

		for (TreeVertex vertex : data.vertexSet()) {
			String ui = vertex.getDescUi();
			String scopenote = data.getDescriptorByVertex(vertex)
					.getScopeNote();
			if (scopenote != null) {
				categoryMap.put(ui, generateWordList(scopenote, wordlist, ui));
			}
		}
		return categoryMap;
	}

	/*
	 * builds the wordlist with number of matches by modersohn
	 */
	private String generateWordList(String scopenote,
			Map<String, Integer> wordlist, String ui) {
		String list = "";

		// find and count the occurrences of interesting words within the
		// scopenote
		Map<String, Integer> wordlistcopy = new HashMap<String, Integer>(
				wordlist);
			String note = scopenote.toLowerCase();
			for (String word : wordlist.keySet()) {

				Pattern p = Pattern.compile(word.toLowerCase());
				Matcher m = p.matcher(note);
				while (m.find()) {
					wordlistcopy.put(word, wordlistcopy.get(word) + 1);
				}
			}

		// build output string with results
		List<String> words = new ArrayList<String>();
		for (String result : wordlistcopy.keySet()) {
			if (0 < wordlistcopy.get(result)) {
				words.add(Pattern.compile(result) + "("
						+ wordlistcopy.get(result) + ")");
			}
		}
		list = StringUtils.join(words, "|");

		return list;
	}
}
