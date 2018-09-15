package de.julielab.concepts.db.creators.mesh.exchange;

import java.util.*;

import org.slf4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Concept;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.Term;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * <p>
 * This class is a SAX handler to parse descriptor written in the old user defined XML format that was used as an
 * intermediate format to import data into the semedico DBMS.
 * </p>
 * 
 * 
 * <p>
 * It's callback based, thus it overrides the appropriate call back methods.
 * </p>
 * 
 * @author Philipp Lucas
 */
public class Parser4UserDefMesh extends DefaultHandler {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(Parser4UserDefMesh.class);

	// for data to import
	private Tree data;

	// the facet descriptor, i.e. root of this branch
	private Descriptor facet = null;

	// currently parsed descriptor record, concept, etc
	private Descriptor desc;
	private Concept concept;
	private String parentUi;

	private ProgressCounter counter;
	private StringBuilder currentBuffer;
	@SuppressWarnings("unused")
	private boolean isFirstVariation = true;

	int cnt1, cnt2;

	/* now we also need some maps to store all parsed data before we can add them into a Tree instance */

	// maps name of parent vertex to a set of all children (remember: within one files there is no polyhierarchy)
	private LinkedHashMap<String, Set<Descriptor>> parentUi2children = new LinkedHashMap<String, Set<Descriptor>>();
	private LinkedHashMap<String, Descriptor> ui2desc = new LinkedHashMap<String, Descriptor>();

	// a counter to create unique vertex names
	Integer nameCounter = new Integer(0);

	// prefix for names
	String namePrefix;

	private List<Descriptor> createdDescriptors = new ArrayList<>();

	/**
	 * Constructor
	 * 
	 * @param data
	 *            The Tree instance to import the data to.
	 * @param fileName
	 *            Name of the file that is parsed with this instance.
	 */
	public Parser4UserDefMesh(Tree data, String fileName) {
		super();
		this.data = data;
		setNamePrefix(fileName);
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
		cnt1 = 0;
		cnt2 = 0;
		nameCounter = 0;
	}

	/**
	 * Called upon end of document
	 */
	@Override
	public void endDocument() {
		counter.finishMsg();

		// We now have to connect terms to the facet root in case their parents were not included in the facet. This
		// is
		// possible when terms are selected for a facet without selecting their parents, too. This is the case for
		// the
		// Transplantation facet, for example: All terms in there specify a parent, so up to now this facet doesn't
		// seem
		// to have a single root.
		Set<Descriptor> newFacetRoots = new HashSet<>();
		// Determine UIs in the parent map that don't have an entry in the descriptor map; this means that the parent
		// is
		// not included in this facet.
		// The children of such terms are actually facet roots.
		for (String ui : parentUi2children.keySet()) {
			Descriptor parentDesc = ui2desc.get(ui);
			if (null == parentDesc) {
				Set<Descriptor> childrenDescs = parentUi2children.get(ui);
				for (Descriptor child : childrenDescs) {
					newFacetRoots.add(child);
				}
			}
		}
		// Now add the newly determined facet roots to the existing facet roots.
		Set<Descriptor> facetRoots = parentUi2children.get(facet.getUI());
		if (null == facetRoots) {
			facetRoots = new HashSet<>();
			parentUi2children.put(facet.getUI(), facetRoots);
		}
		facetRoots.addAll(newFacetRoots);

		logger.info("# Building up preliminary tree ... ");
		counter = new ProgressCounter(0, 100, "tree vertex");
		counter.startMsg();
		buildPreliminaryTreeStructure(facet, facet.getTreeVertices().iterator().next().getName(), data);
		// buildPreliminaryTreeStructure(data.getRootDesc(), data.getRootVertex().getName(), data);
		counter.finishMsg();
		logger.info("# ... done. ");
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
		if (localName.equals("terms")) {
			String facetName = atts.getValue("facet");
			if (facetName == null) {
				logger.warn("no facet specified! I'm using 'no-facet' instead.");
				facetName = "no-facet";
			}
			addFacet(data, facetName);

		} else if (localName.equals("term")) {
			desc = new Descriptor();
			desc.setUI(atts.getValue("id"));
			parentUi = atts.getValue("parent-id");
			// if there is no parent-id it must be the root of this branch
			// -> a direct child of the "facet root"
			if (parentUi == null) {
				parentUi = facet.getUI(); // data.getRootDesc().getUI();
			}

		} else if (localName.equals("variations")) {
			isFirstVariation = true;
			concept = new Concept(false);
		}

		// and in any case clear the stringbuffer for currently read text
		currentBuffer.setLength(0);
	}

    public List<Descriptor> getCreatedDescriptors() {
        return createdDescriptors;
    }

    @Override
	public void endElement(String uri, String localName, String qName) throws SAXException {

		if (localName.equals("canonic")) {
			// only one concept is created for synonyms and canonic name. This
			// is the prefConcept.
			// also the canonic name is prefTerm.
			concept = new Concept(true);
			concept.addTerm(new Term(getCurrentText(), true));

		} else if (localName.equals("description")) {
			desc.setScopeNote(getCurrentText());

		} else if (localName.equals("synonym")) {
			concept.addTerm(new Term(getCurrentText(), false));

		} else if (localName.equals("synonyms")) {
			// another concept is added for variations, however this is not the
			// preferred concept
			if (concept.size() > 0) {
				desc.addConcept(concept);
				concept = null;
			}
		} else if (localName.equals("organism")) {
			// don't know!?
			// TODO find out

		} else if (localName.equals("shortDescription")) {
			// TODO: not needed for now
			// sometimes it got a value e.g. for proteins.xml

		} else if (localName.equals("type")) {
			// not needed for now
			// TODO find out what this is good for
			// value is e.g.: de.julielab.jules.types.MeshHeading

		} else if (localName.equals("variations")) {
			if (concept.size() > 0) {
				desc.addConcept(concept);
				concept = null;
			}

			// } else if (localName.equals("token")) {
			// // "token" appears only within <variation> </variation>, thus it's
			// // ok to look for "token" like this
			// concept.addTerm(new Term(getCurrentText(), isFirstVariation));
			// if (isFirstVariation) {
			// isFirstVariation = false;
			// }

		} else if (localName.equals("term")) {
			counter.inc();

			// update parentUi2children map
			Set<Descriptor> descSet = parentUi2children.get(parentUi);
			if (descSet == null) {
				descSet = new LinkedHashSet<Descriptor>();
				parentUi2children.put(parentUi, descSet);
			}
			descSet.add(desc);

			// update ui2desc map
			ui2desc.put(desc.getUI(), desc);
            createdDescriptors.add(desc);


			// DEBUG
			if (desc.getConcepts().size() > 1) {
				cnt2++;
			} else {
				cnt1++;
			}
		}

		currentBuffer.setLength(0);
	}

	/**
	 * <p>
	 * Recursive method to build preliminary tree structure from the data parsed. "temporary" means that it devolves the
	 * parsed data into a Tree instance, however, it is not the final result yet. Next step will be to restore the
	 * original tree-numbers as well as the operations done on the original MeSH which resulted in this version of the
	 * MeSH. This process is be done in another class.
	 * </p>
	 * 
	 * <p>
	 * To use it just call it with the root vertex and the instance of Tree where the preliminary structure should be
	 * created. See source code for more details of how it works...
	 * <p>
	 * 
	 * @param parent
	 *            Descriptor of the branch-root vertex of the vertices which will be added.
	 * @parentVertexName Name of the branch-root vertex of the vertices which will be added.
	 * @param tree
	 *            a <code>Tree instance</code>.
	 */
	private void buildPreliminaryTreeStructure(Descriptor parent, String parentVertexName, Tree tree) {
		String vertexName;
		counter.inc();

		// abort if we reached a leaf of the tree
		if (!parentUi2children.containsKey(parent.getUI())) {
			return;
		}

		// get descriptors that belong to children of %parent
		for (Descriptor desc : parentUi2children.get(parent.getUI())) {

			// create artificial tree number / name and increment nameCounter
			if (parent.getName().equals(facet.getName())) {
				// if (tree.isRoot(parent)) {
				// this way getTreeNumber will always reveal which file the vertex came from...
				vertexName = namePrefix.replace(".", "_") + (++nameCounter).toString();
				// logger.info("Parsed direct child of root: " + desc.getName() + " : " + desc.getUI());

			} else {
				vertexName = namePrefix + (++nameCounter).toString();
			}

			// add descriptor (using vertex names)
			if (!tree.hasDescriptorByUi(desc.getUI())) {
				tree.addDescriptor(desc, parentVertexName, vertexName);
			} else {
				tree.addTreeVertexToDesc(desc.getUI(), parentVertexName, vertexName);
			}

			// depth first :-)
			buildPreliminaryTreeStructure(desc, vertexName, tree);
		}
	}

	/**
	 * Sets the prefix for the names of all vertices parsed. "art" to indicate that these are artificial names and not
	 * real tree numbers and then the first two character of the file which is parsed. I checked that they are unique.
	 */
	private void setNamePrefix(String fileName) {
		namePrefix = "art." + fileName.substring(0, 2) + ".";
	}

	/**
	 * @return Returns the trimmed current content of the currentBuffer StringBuffer
	 */
	private String getCurrentText() {
		return currentBuffer.toString().trim();
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
	private void addFacet(Tree data, String facetName) {
		// We must append "facet" to avoid a name collision between facet and descriptor with name "Gene Expression"
		String name = "Facet " + facetName;
//		if (data.hasDescriptorByName(name)) {
//		    logger.debug("Facet node with name {} already existed and is reused.", name);
//            facet = data.getDescriptorByName(name);
//            return;
//        }

		facet = new Descriptor();
		facet.setSemedicoFacet(true);

		concept = new Concept(true);
		concept.addTerm(new Term(name, true));
		facet.addConcept(concept);

		// facet.setName(facetName);
		facet.setUI("F_" + facetName);
		facet.setScopeNote("This is the descriptor of facet '" + facetName + "'.");

		data.addDescriptor(facet, data.getRootVertex().getName(), name);
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
