package de.julielab.semedico.mesh.components;

/**
 * Instances of this class represent a term of a of a concept of a descriptor.
 * see also http://www.nlm.nih.gov/mesh/xml_data_elements.html#Term
 * 
 * @author Philipp Lucas
 */
public class Term {
	private String name;
	private String ID; // http://www.nlm.nih.gov/mesh/xml_data_elements.html#TermUI

	// is it the preferred term of the concept it belongs to?
	private boolean preferred;

	/**
	 * Copy constructor
	 * @param t Term to copy.
	 */
	public Term(Term t) {
		setName(t.name);
		setID(t.ID);
		this.preferred = t.preferred;
	}
	
	public Term(String term, boolean pref) {
		setName(term);
		this.preferred = pref;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		if( name == null) {
			throw new IllegalArgumentException();
		}
		this.name = name;
	}

	public String getID() {
		return ID;
	}

	public void setID(String iD) {
		ID = iD;
	}

	/**
	 * @return Returns true if it is the preferred term of the concept it
	 *         belongs to, else false.
	 */
	public boolean isPreferred() {
		return preferred;
	}
	
	public void setPreferred(boolean preferred ) {
		this.preferred = preferred;
	}

}
