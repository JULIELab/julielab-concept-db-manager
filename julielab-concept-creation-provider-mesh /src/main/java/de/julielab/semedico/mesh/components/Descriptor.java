package de.julielab.semedico.mesh.components;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.semedico.mesh.Tree;

/**
 * Represents a (part of a) descriptor-record as in
 * http://www.nlm.nih.gov/mesh/xml_data_elements.html#DescriptorRecord
 * 
 * @author Philipp Lucas
 */
public class Descriptor implements Comparable <Descriptor>{
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(Descriptor.class);
	
	// (unique) name of a descriptor. It equals the name of the preferred term of the preferred concept
    // private String name; -> see getName() below
    
    // the set of concepts of a descriptor. These includes a set of terms for each concept
    private List<Concept> concepts;    
    
    // set of vertices in the internal tree -> from these the tree number can be calculated -> getTreeNumbers()
    private List<TreeVertex> treeVertices;
    
    private String ui;
    
    private String scopeNote;

    private boolean isSemedicoFacet;
    
    /**
     * normal constructor
     */
    public Descriptor() {
    	super();
    	concepts = new ArrayList<Concept>();    
    	treeVertices = new ArrayList<TreeVertex>();
    	ui = "";
    	scopeNote = "";    	
    }    
    
	/**
	 * Copy constructor. Creates a new instance of Descriptor containing the
	 * "gleichen aber nicht selben" data.
	 * 
	 * @param desc Descriptor to copy.
	 * @param copyVertices True to copy vertices as well, false otherwise.
	 */
    public Descriptor(Descriptor desc, boolean copyVertices) {
    	super();
    	this.ui = desc.ui;
    	this.scopeNote = desc.scopeNote;
    	treeVertices = new ArrayList<TreeVertex>();
    	if(copyVertices){
    		for(TreeVertex v : this.treeVertices) {
    			treeVertices.add(new TreeVertex(v));
    		}
    	}
    	concepts = new ArrayList<Concept>();
    	for(Concept c : desc.concepts) {
    		concepts.add(new Concept(c));
    	}
    }
    
	/**
	 * Copy constructor. Creates a new instance of Descriptor containing the
	 * "gleichen aber nicht selben" data.
	 * 
	 * @param desc Descriptor to copy.
	 */
    public Descriptor(Descriptor desc) {
    	super();
    	this.ui = desc.ui;
    	this.scopeNote = desc.scopeNote;
    	treeVertices = new ArrayList<TreeVertex>();
    	
    	for(TreeVertex v : this.treeVertices) {
    		treeVertices.add(new TreeVertex(v));
    	}
    	concepts = new ArrayList<Concept>();
    	for(Concept c : this.concepts) {
    		concepts.add(new Concept(c));
    	}
    }
    
    /**
     * @param term The concept to add. You cannot 2nd preferred concept.
     * @return True if concept was successfully added, false if the concept is already contained.
     */
    public boolean addConcept(Concept concept) {
        if (concept.isPreferred() && getPrefConcept() != null) {
        	return false;
        }
    	return concepts.add(concept);
    }

    public List<Concept> getConcepts() {
        return concepts;
    }
    
    public Concept getPrefConcept() {
        for(Concept concept : concepts ) {
            if (concept.isPreferred()) {
                return concept;
            }
        }
        return null;
    }
    
    /**
     * @param term The vertex to add.
     * @return True if vertex was successfully added, false if the vertex is already contained.
     */
    public boolean addTreeVertex(TreeVertex vertex) {
        return treeVertices.add(vertex);
    }
    
    public List<TreeVertex> getTreeVertices() {
        return treeVertices;
    }
    
    public boolean hasTreeVertices() {
    	return !treeVertices.isEmpty();
    }
    
    public boolean removeTreeVertex(TreeVertex vertex) {
        return treeVertices.remove(vertex);
    }
    
    /**
	 * @param nameOfTerm
	 *            Name of the term to search for. 
	 * @return Returns
	 *            If any concept of that descriptor contains a
	 *            term with a name that equals <code>nameOfTerm</code> that concept is returned.
	 *            Otherwise it returns <code>null</code>.
	 */
    public Concept hasTerm(String nameOfTerm) {
    	for(Concept c : concepts) {
    		Term t = c.getTermByName(nameOfTerm);
    		if ( t != null) {
    			return c;
    		}
    	}
    	return null;
    }

    public String getName() {
    	Concept prefConcept = getPrefConcept();
    	if ( prefConcept == null) {
    		logger.error("prefconcept == null ! desc was : " + getUI());
    	}
    	
    	Term prefTerm = prefConcept.getPrefTerm();
    	if ( prefTerm == null) {
    		logger.error("prefTerm == null ! desc was : " + getUI());
    	}
    	
    	String name = prefTerm.getName();
    	if ( name == null) {
    		logger.error("name == null ! desc was : " + getUI());
    	}
    	
    	return name;
    }

    /**
	 * Sets the name (i.e. the name of the preferred term of the preferred
	 * concept of this descriptor) to <code>name</code>.
	 * 
	 * @param name
	 */
    public void setName(String name) {
		// TODO: problem: name of descriptor is in fact name of its preferred
		// term ... what should we do here?
		// workaround: if a term with that name is there, select it as preferred
		// term. Otherwise: add new term and set it to be preferred.
    	
    	// preferred concept of new preferred term
    	Concept c = hasTerm(name);
    	
    	// new preferred term 
    	Term t;
    	
    	if (c != null) {    		    		
    		t = c.getTermByName(name);
    	}
    	
    	else {
    		// damn... in which concept should we add it?
    		// let's use the preferred concept
    		c = getPrefConcept();
    		t = new Term(name, false);
    		c.addTerm(t);
    	}
    	
		// set c as new preferred concept
		Concept prefConcept = getPrefConcept();
		prefConcept.setPreferred(false);
		c.setPreferred(true);
		
		// set t as new preferred term of c
		c.getPrefTerm().setPreferred(false);	    		    		    		
		t.setPreferred(true);
    }
    
    public String getUI() {
		return ui;
	}

	public void setUI(String ui) {
		this.ui = ui;
	}
	
	/**
	 * This method is for convenient retrieval of all synonyms of a descriptor. A synonyms is a name of a term of a concept of this descriptor. 
	 * @return Returns a list containing the names of all synonyms of this descriptor.
	 */
	public List<String> getSynonymNames() {
		List<String> list = new LinkedList<String>();
		for(Concept c : getConcepts()) {
			for(Term t : c.getTerms()) {
				list.add(t.getName());
			}
		}
		return list;
	}
	
	public String getScopeNote() {
		return scopeNote;
	}

	public void setScopeNote(String scopeNote) {
		this.scopeNote = scopeNote;
	}  
	
	/**
	 * @return Returns a very simple String representation of this descriptor.
	 */
	public String toString() {
		return this.getName() + " : " + this.getUI();
	}
	
	/**
	 * @param  data The tree that contains this descriptor.
	 * @return Returns a full String representation of this descriptor.
	 */
	public String tofullString(Tree data) {
		StringBuilder str = new StringBuilder();
		str.append("DescriptorName" + "\t" + getName() + "\n");
		str.append("DescriptorUI" + "\t" + getUI() + "\n");

		str.append("Tree Vertices\n");
		for (TreeVertex vertex : getTreeVertices()) {
			str.append(vertex.toFullString(data));
		}

		str.append("Concepts\n");
		for (Concept c : getConcepts()) {
			str.append("Concept" + "\t" + c.getPrefTerm().getName() + "\n");
			for (Term t : c.getTerms()) {
				str.append(" TermName" + "\t" + t.getName() + "\n");
				str.append(" TermUI" + "\t" + t.getID() + "\n");
			}
		}
		return str.toString();
	}

	public boolean isSemedicoFacet() {
		return isSemedicoFacet;
	}

	public void setSemedicoFacet(boolean isSemedicoFacet) {
		this.isSemedicoFacet = isSemedicoFacet;
	}

	@Override
	public int compareTo(Descriptor o) {
		return this.ui.compareTo(o.ui);
	}

}
