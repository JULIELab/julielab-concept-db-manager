package de.julielab.concepts.db.creators.mesh.exchange;

import org.slf4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import de.julielab.concepts.db.creators.mesh.components.Concept;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.Term;
import de.julielab.concepts.db.creators.mesh.components.VertexLocations;
import de.julielab.concepts.db.creators.mesh.modifications.DescAdditions;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * <p>
 * This class is a SAX handler to parse descriptor records from OwnMeSH XML
 * data. It's callback based, thus it overrides the appropriate call back
 * methods.
 * </p>
 * <p>
 * Other than the necessary call-back methods it provides mainly one method:
 * <code>DescAdditions getNewDescriptors()</code>, which returns the new/added
 * descriptors.
 * </p>
 * <p>
 * This parses the "OwnMeSH" XML format. It is used for only one purpose so far:
 * When determining the changes that led to the "old semedico MeSH" newly added
 * descriptors are found. These are stored in this format.
 * </p>
 * <p>
 * The format is a subset of the OrigMeSH, reducing the XML-Elements to those
 * actually represented by <code>Tree</code>. In addition, there is one change
 * as follows:
 * <ul>
 * <li>location is given explicitly as a parent-child relation.</li>
 * <li>location is renamed: TreeNumberList -> LocationList</li>
 * </ul>
 * Reason for this change is, that we don't want Tree-Number to implicitly
 * represent the structure of our data, but instead want explicit relations for
 * it. The above modification allows for that.
 * </p>
 * 
 * @author Philipp Lucas
 */
public class Parser4OwnMesh extends DefaultHandler {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(Parser4OwnMesh.class);
	
    // to store new descriptors
	private DescAdditions newDescs = new DescAdditions();
    
    // currently parsed descriptor record, concept, etc
    private Descriptor desc;
    private Concept concept;
    private Term term;
    private VertexLocations child2parentVertexNames;
    private boolean isPrefTerm;
    private String childVertexName;
    private ProgressCounter counter;
    private StringBuilder currentBuffer;
    private String nameBuffer;

    /**
     * Constructor
     * @param data The Tree instance to import the data to.
     */
    public Parser4OwnMesh() {
		super();
	}

	/*
     * (non-Javadoc)
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
            child2parentVertexNames = new VertexLocations();            
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
            newDescs.put(desc, child2parentVertexNames);
            counter.inc();
            desc = null;
        } else if (localName.equals("VertexName")) {
        	childVertexName = getCurrentText();          
        } else if (localName.equals("ParentVertexName")) {
        	child2parentVertexNames.put(childVertexName,getCurrentText());
        	childVertexName = null;
        } else if (localName.equals("Concept")) {
            desc.addConcept(concept);
        } else if (localName.equals("Term")) {
        	term.setName(nameBuffer);
            concept.addTerm(term);
        } else if (localName.equals("DescriptorUI")) {
        		desc.setUI(getCurrentText());
        } else if (localName.equals("ScopeNote")) {
        	desc.setScopeNote(getCurrentText());    
        } else if (localName.equals("String")) {
        	nameBuffer = getCurrentText();
        }
        currentBuffer.setLength(0);
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
    
    public DescAdditions getNewDescriptors() {
    	return newDescs;
    }
    
}
