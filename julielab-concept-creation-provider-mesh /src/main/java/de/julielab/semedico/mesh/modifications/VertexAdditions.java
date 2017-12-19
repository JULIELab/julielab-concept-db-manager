package de.julielab.semedico.mesh.modifications;

import java.util.LinkedHashMap;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.tools.ProgressCounter;

/**
* <p>
 * A class to handle vertex additions. It stores them and can apply them to
 * a <code>Tree</code> object.
 * </p>
 * 
 * @author Philipp Lucas
 */

public class VertexAdditions 
implements TreeModficationsInterface {
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(VertexAdditions.class);
	
	private LinkedHashMap<String, String> newVertex2parent = new LinkedHashMap<String, String>();
	private LinkedHashMap<String, String> newVertex2descUi= new LinkedHashMap<String, String>();
	
	public void put(String vertexName, String parentVertexName, String descUi) {
		
		// check for existing mapping
		if (newVertex2descUi.containsKey(vertexName)) {
			logger.warn("Overwriting existing vertex addition " + toString(vertexName));
		}
		
		newVertex2parent.put(vertexName, parentVertexName);
		newVertex2descUi.put(vertexName, descUi);
	}
	
	public String getParentVertexName(String vertexName) {
		return newVertex2parent.get(vertexName);
	}
	
	public String getDescUi(String vertexName) {
		return newVertex2descUi.get(vertexName);
	}
	
	public Set<String> keySet() {
		return newVertex2parent.keySet();
	}
	
	public int size() {
		return newVertex2parent.size();
	}
	
	@Override
	public boolean isEmpty() {
		return newVertex2parent.isEmpty();
	}
	
	public boolean contains(String vertexName) {
		return newVertex2descUi.containsKey(vertexName);
	}
	
	/**
	 * Adds the additions currently hold in this object to <code>data</code>.
	 * @param data A <code>Tree</code> object.
	 */
	public void apply(Tree data) {
		logger.info("# Adding vertices to " + data.getName() + " ...");
		ProgressCounter counter = new ProgressCounter(newVertex2parent.size(), 10, "vertex");
	
		for(String vertexName : newVertex2parent.keySet()) {
			data.addTreeVertexToDesc(getDescUi(vertexName), getParentVertexName(vertexName), vertexName);			
			counter.inc();
		}
		logger.info("# ... done.");
	}
	
	public String toString(String vertexName) {
		if(contains(vertexName)) {
			return "added vertex : " + vertexName + " --- parent : " + getParentVertexName(vertexName) + " --- descUI : " + getDescUi(vertexName);			
		} else {
			return "";
		}
	}
	
	@Override
	public String toString() {
		StringBuffer out = new StringBuffer();
		for(String v : keySet()) {
			out.append(toString(v) + "\n");
		}
		return out.toString();
	}

}
