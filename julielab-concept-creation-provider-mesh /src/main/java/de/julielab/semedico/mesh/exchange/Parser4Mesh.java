package de.julielab.semedico.mesh.exchange;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.components.Concept;
import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.Term;
import de.julielab.semedico.mesh.components.TreeNumber;
import de.julielab.semedico.mesh.components.TreeVertex;
import de.julielab.semedico.mesh.tools.ProgressCounter;

/**
 * This class is a SAX handler to parse descriptor records from original MeSH XML data. It's callback based, thus it
 * overrides the appropriate call back methods.
 * 
 * @author Philipp Lucas
 */
public class Parser4Mesh extends DefaultHandler {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(Parser4Mesh.class);

	// for all imported data
	private Tree data;

	/**
	 * stores all tree numbers of a descriptor records. This is necessary since tree-numbers are derived from the
	 * position in the data of our internal graph model, instead of storing static values. For building up the data,
	 * however, tree numbers are needed/useful.
	 **/
	private List<TreeNumber> treeNumbers;

	// currently parsed descriptor record, concept, etc
	private Descriptor desc;
	private Concept concept;
	private Term term;
	private boolean isPrefTerm;
	private ProgressCounter counter;
	private StringBuilder currentBuffer;
	private String nameBuffer;

	// this is important! there are more than one occurrences of DescriptorUI in a
	// DescriptorRecord - but we only want to parse the first one!
	private boolean descUiParsedflag;

	/**
	 * Constructor
	 * 
	 * @param data
	 *            The Tree instance to import the data to.
	 * @param createMeshFacets
	 */
	public Parser4Mesh(Tree data, boolean createMeshFacets) {
		super();
		this.data = data;
		if (createMeshFacets)
			addMeshFacets(data);
	}

	/**
	 * Constructor
	 * 
	 * @param data
	 *            The Tree instance to import the data to.
	 * @param createMeshFacets
	 */
	public Parser4Mesh(Tree data) {
		this(data, false);
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

	/**
	 * Called upon start of document
	 */
	@Override
	public void startDocument() {
		counter = new ProgressCounter(0, 1000, "descriptor record");
		counter.startMsg();
		currentBuffer = new StringBuilder();
	}

	/**
	 * Called upon end of document
	 */
	@Override
	public void endDocument() {
		counter.finishMsg();
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
		if (localName.equals("DescriptorRecord")) {
			desc = new Descriptor();
			treeNumbers = new ArrayList<TreeNumber>();
			descUiParsedflag = false;
		} else if (localName.equals("TreeNumberList")) {
			// moved to DescriptorRecord above
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

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (localName.equals("DescriptorRecord")) {
			data.addDescriptor(desc, treeNumbers);
			counter.inc();
			desc = null;
			treeNumbers = null;
		} else if (localName.equals("TreeNumber")) {
			TreeNumber nr = new TreeNumber(getCurrentText());
			treeNumbers.add(nr);
		} else if (localName.equals("Concept")) {
			desc.addConcept(concept);
		} else if (localName.equals("Term")) {
			term.setName(nameBuffer);
			concept.addTerm(term);
		} else if (localName.equals("DescriptorUI")) {
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

	private void addMeshFacets(Tree data) {
		addFacet(data, "Facet Anatomy Anatomy", "A");
		addFacet(data, "Facet Organisms Organisms", "B");
		addFacet(data, "Facet Diseases Diseases", "C");
		addFacet(data, "Facet Chemicals and Drugs Chemicals and Drugs", "D");
		addFacet(
				data,
				"Facet Analytical, Diagnostic and Therapeutic Techniques and Equipment Analytical, Diagnostic and Therapeutic Techniques and Equipment",
				"E");
		addFacet(data, "Facet Psychiatry and Psychology Psychiatry and Psychology", "F");
		addFacet(data, "Facet Phenomena and Processes Phenomena and Processes", "G");
		addFacet(data, "Facet Disciplines and Occupations Disciplines and Occupations", "H");
		addFacet(
				data,
				"Facet Anthropology, Education, Sociology and Social Phenomena Anthropology, Education, Sociology and Social Phenomena",
				"I");
		addFacet(data, "Facet Technology, Industry, Agriculture Technology, Industry, Agriculture", "J");
		addFacet(data, "Facet Humanities Humanities", "K");
		addFacet(data, "Facet Information Science Information Science", "L");
		addFacet(data, "Facet Named Groups Named Groups", "M");
		addFacet(data, "Facet Health Care Health Care", "N");
		addFacet(data, "Facet Publication Characteristics Publication Characteristics", "V");
		addFacet(data, "Facet Geographicals Geographicals", "Z");
	}

	/**
	 * Adds a facet named <code>facetName</code> to <code>data</code> by adding a descriptor with:
	 * <ul>
	 * <li>name = <code>"Facet" + " " + facetName</code></li>
	 * <li>ui = <code>"D_" + facetName</code></li>
	 * <li>a single tree vertex with name <code>facetName</code>
	 * <li>etc (see source)</li>
	 * </ul>
	 * 
	 * @param data
	 * @param facetName
	 */
	private void addFacet(Tree data, String facetName, String treeNumber) {
		// We must append "facet" to avoid a name collision between facet and descriptor with name "Gene Expression"
		String name = "Facet " + facetName;

		Descriptor facet = new Descriptor();
		facet.setSemedicoFacet(true);

		concept = new Concept(true);
		concept.addTerm(new Term(name, true));
		facet.addConcept(concept);

		// facet.setName(facetName);
		facet.setUI("F_" + facetName);
		facet.setScopeNote("This is the descriptor of facet '" + facetName + "'.");

		List<TreeNumber> treeNumbers = new ArrayList<>();
		treeNumbers.add(new TreeNumber(treeNumber));
		data.addDescriptor(facet, treeNumbers);
	}

	/**
	 * @return Returns the trimmed current content of the currentBuffer StringBuffer
	 */
	private String getCurrentText() {
		return currentBuffer.toString().trim();
	}

	@Override
	public void error(SAXParseException e) throws SAXException {
		logger.error("SAX parse exception: " + e.getMessage());
	}

	@Override
	public void warning(SAXParseException e) throws SAXException {
		logger.warn("SAX parse warning: " + e.getMessage());
	}

	@Override
	public void skippedEntity(String name) throws SAXException {
		logger.warn("SAX skipped entity: " + name);
	}

}
