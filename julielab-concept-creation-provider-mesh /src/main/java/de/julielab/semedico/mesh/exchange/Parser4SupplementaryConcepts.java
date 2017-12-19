package de.julielab.semedico.mesh.exchange;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.components.Concept;
import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.Term;
import de.julielab.semedico.mesh.components.TreeVertex;
import de.julielab.semedico.mesh.tools.ProgressCounter;

public class Parser4SupplementaryConcepts extends DefaultHandler {

	private static final Logger log = LoggerFactory.getLogger(Parser4SupplementaryConcepts.class);

	// for all imported data
	private Tree data;

	private Descriptor desc;
	private Concept concept;
	private boolean isPrefTerm;
	private Term term;
	private ProgressCounter counter;
	private StringBuilder currentBuffer;
	private boolean descUiParsedflag;
	private String nameBuffer;
	private List<String> descriptorsReferredTo;
//	private Multimap<String, String> supplementaryChildrenCounts;
	private int numConceptsWithoutParent;
	private int suppVertexNodeCounter;

	public Parser4SupplementaryConcepts(Tree data) {
		super();
		this.data = data;
//		this.supplementaryChildrenCounts = HashMultimap.create();
		this.numConceptsWithoutParent = 0;
		this.suppVertexNodeCounter = 0;
	}

	/**
	 * Called upon start of document
	 */
	@Override
	public void startDocument() {
		counter = new ProgressCounter(0, 20000, "supplementary concept record");
		counter.startMsg();
		currentBuffer = new StringBuilder();
		descriptorsReferredTo = new ArrayList<>();
	}

	@Override
	public void endDocument() throws SAXException {
		counter.finishMsg();
		log.info("{} of {} Supplementary Concepts do not have a parent in the passed data tree and were not appended.",
				numConceptsWithoutParent, counter.getCount());
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
		if (localName.equals("SupplementalRecord")) {
			desc = new Descriptor();
			descriptorsReferredTo.clear();
			descUiParsedflag = false;
		} else if (localName.equals("Concept")) {
			boolean isPrefConcept = atts.getValue("PreferredConceptYN").equals("Y");
			concept = new Concept(isPrefConcept);
		} else if (localName.equals("Term")) {
			isPrefTerm = atts.getValue("ConceptPreferredTermYN").equals("Y");
			term = new Term(getCurrentText(), isPrefTerm);
		}
		// and in any case clear the stringbuffer for currently read text
		currentBuffer.setLength(0);
	}

	/**
	 * @return Returns the trimmed current content of the currentBuffer StringBuffer
	 */
	private String getCurrentText() {
		return currentBuffer.toString().trim();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		currentBuffer.append(ch, start, length);
	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (localName.equals("SupplementalRecord")) {
			boolean atLeastOneParentFound = false;
			// List<TreeNumber> treeNumbers = new ArrayList<>();
			List<String> suppVertexNames = new ArrayList<>();
			List<String> parentTreeVertexNames = new ArrayList<>();
			for (String referredUI : descriptorsReferredTo) {
				// An asterisk at the beginning of a UI indicates that this is the major topic descriptor. Currently, we
				// ignore the difference between minor and major topic. Perhaps it could be of some use in the future.
				referredUI = referredUI.replaceFirst("\\*", "");
				Descriptor referredDescriptor = data.getDescriptorByUi(referredUI);
				if (null != referredDescriptor) {
					atLeastOneParentFound = true;
					List<TreeVertex> referredTreeVertices = referredDescriptor.getTreeVertices();
					for (TreeVertex referredTreeVertex : referredTreeVertices) {
						String parentTreeVertexName = referredTreeVertex.getName();
//						String suppVertexName = "supp" + supplementaryChildrenCounts.get(parentTreeVertexName).size();
						String suppVertexName = "supp" + (++suppVertexNodeCounter);
						suppVertexNames.add(suppVertexName);
//						supplementaryChildrenCounts.put(parentTreeVertexName, suppVertexName);
						parentTreeVertexNames.add(parentTreeVertexName);
					}
				}
			}
			if (!atLeastOneParentFound) {
				numConceptsWithoutParent++;
				// log.warn("The concept with UI " + desc.getUI()
				// + " does not have any parents in the passed data tree and will not be appended.");
			} else {
//				if (null == suppVertexNames)
//					throw new IllegalStateException(
//							"The vertex name of the Supplementary Concept "
//									+ desc
//									+ " is null what should not happen. This name is generated automatically and not read from the data, so this is a program error.");
				data.addDescriptor(desc, parentTreeVertexNames.toArray(new String[parentTreeVertexNames.size()]),
						suppVertexNames.toArray(new String[suppVertexNames.size()]));
			}
			counter.inc();
			desc = null;
		} else if (localName.equals("DescriptorUI")) {
			descriptorsReferredTo.add(getCurrentText());
		} else if (localName.equals("Concept")) {
			desc.addConcept(concept);
		} else if (localName.equals("Term")) {
			term.setName(nameBuffer);
			concept.addTerm(term);
		} else if (localName.equals("SupplementalRecordUI")) {
			if (!descUiParsedflag) {
				desc.setUI(getCurrentText());
				descUiParsedflag = true;
			}
		} else if (localName.equals("TermUI")) {
			term.setID(getCurrentText());
		} else if (localName.equals("ScopeNote")) {
			desc.setScopeNote(getCurrentText());
		} else if (localName.equals("String")) {
			nameBuffer = getCurrentText();
		}
		currentBuffer.setLength(0);
	}
}
