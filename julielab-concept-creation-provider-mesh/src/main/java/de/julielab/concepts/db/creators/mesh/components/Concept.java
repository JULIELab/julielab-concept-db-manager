package de.julielab.concepts.db.creators.mesh.components;

import java.util.ArrayList;
import java.util.List;

/**
 * Instances of this class represent a concept of a descriptor
 * see also http://www.nlm.nih.gov/mesh/xml_data_elements.html#Concept 
 * 
 * The preferred term of a concept can only be set indirectly by adding a term to the 
 * concept which is preferred.
 * 
 * @author Philipp Lucas
 */

public class Concept {
    private List<Term> terms;
    private boolean preferred;

    public Concept() {
    	terms = new ArrayList<Term>();
        this.preferred = false;
    }   
    
    public Concept(boolean preferred) {
    	terms = new ArrayList<Term>();
        this.preferred = preferred;
    }   
    
    /**
     * Copy constructor
     * @param c Concept to copy.
     */
    public Concept(Concept c) {
    	this.preferred = c.preferred;
    	terms = new ArrayList<Term>(c.size());    	
    	for (Term t : c.terms) {
    		this.addTerm(new Term(t));
    	}
    }
    
    public boolean isPreferred() {
        return preferred;
    }

    public void setPreferred(boolean preferred) {
        this.preferred = preferred;
    }
    
    /**
     * @return Returns the preferred term of this concept, or null if there is no preferred term.
     */
    public Term getPrefTerm() {
       for(Term term : terms) {
            if (term.isPreferred()) {
                return term;
            }
        }
        return null;
    }

    public List<Term> getTerms() {
        return terms;
    }
    
    /**
	 * @param name
	 *            Name of term to look for.
	 * @return If this concept contains a term thats name equals
	 *         <code>name</code>, then this term is returned. Otherwise
	 *         <code>null</code> is returned.
	 */
    public Term getTermByName(String name) {
    	for (Term t : getTerms()) {
    		if( t.getName().equals(name)) {
    			return t;
    		}
    	}
    	return null;
    }

    /**
     * @param term The term to add. It is not allowed to contain two term that equal in its names or UIs.
     * @return True if term was successfully added, false if the term is already contained (see above).
     */
    public boolean addTerm(Term term) {
    	for(Term t : terms) {
    		if(t.getName().equals(term.getName())) {
    			return false;
    		} else {
    			String id1=term.getID();
    			String id2=t.getID();
    			if( id1!=null && id2!=null && id1.equals(id2)) {
    				return false;
    			}
    		}
    	}
        return terms.add(term);
    }
    
    /**
     * @param term The term to delete.
     * @return True if term was successfully deleted, false if the term wasn't contained.
     */
    public boolean removeTerm(Term term) {
        return terms.remove(term);
    }
    
    /**
     * @param termName The name of the term to remove.
     * @return true if successfully removed, false if there is no term with that name.
     */
    public boolean removeTerm(String termName) {
       for(Term term : terms) {
            if (term.getName().equals(termName)) {
                return removeTerm(term);
            }
        }
        return false;
    }
    
    /**
     * @param terms Sets the given set of terms as the terms of a concept. This will overwrite any other terms 
     * assigned.
     */
    public void setTerms(List<Term> terms) {
        this.terms = terms;
    }
    
    /**
     * @return Returns the size of the concept, i.e. the number of terms in it.
     */
    public int size() {
    	return terms.size();
    }
    
}
