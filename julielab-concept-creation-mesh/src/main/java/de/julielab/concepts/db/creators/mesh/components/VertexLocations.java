package de.julielab.concepts.db.creators.mesh.components;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

/**
 * <p>
 * This class represents the locations of a descriptor in a MeSH-Tree. Locations
 * in this sense is a set of pairs of tree-vertex and corresponding
 * parent-vertex. With these information it is possible to correctly insert a
 * descriptor in a given Tree object at the correct location.
 * </p>
 * 
 * <p>Usually descriptor location are managed within a Tree object. However, when
 * descriptors are dealt with outside of a Tree object, there is no uniform way
 * of handling locations. Now, this class provides a uniform way of
 * handling.</p>
 * 
 * <p>
 * Since each tree-vertex has a unique name, VertexLocations handles these
 * names, not the vertices itself.
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */
public class VertexLocations {
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(VertexLocations.class);
	
	// maps child vertex names to parent vertex names! 
	private LinkedHashMap<String,String> locs;
	
	public VertexLocations () {
		locs = new LinkedHashMap<String,String>();
	}
	
	public VertexLocations(String vertexName, String parentVertexName) {
		locs = new LinkedHashMap<String,String>();
		locs.put(vertexName, parentVertexName);
	}
	
//	/**
//	 * Copy constructor.
//	 */
//	public VertexLocations (VertexLocations locs) {
//		for(String v : locs.getVertexNameSet()) {
//			this.locs.put(v, locs.get(v));
//		}
//	}
	
	/**
	 * @param vertexName A vertex name.
	 * @param parentVertexName Name of the parent of the vertex with vertex name <code>vertexName</code>.
	 * @return Returns true if successfully added, false otherwise, e.g. if there is already a vertex with that name.
	 */
	public boolean put(String vertexName, String parentVertexName) {
		if(locs.containsKey(vertexName)) {
			logger.warn("Cannot add location because there is already a vertex with that name. Name is " + vertexName);
			return false;
		}
		locs.put(vertexName, parentVertexName);
		return true;
		
	}
	
	/**
	 * @param vertexName Name of vertex for which the name of the parent is to be returned. 
	 * @return Name of parent vertex or null if there is no such vertex.
	 */
	public String get(String vertexName) {
		return locs.get(vertexName);
	}
	
	/**
	 * @param vertexName A vertex name.
	 * @return Returns true if it contains a location for vertex with name <code>vertexName</code>.
	 */
	public boolean has(String vertexName) {
		return locs.containsKey(vertexName);
	}
	
	/**
	 * Removes the location of vertex with name <code>vertexName</code>.
	 * @param vertexName Name of a vertex.
	 */
	public void remove(String vertexName) {
		locs.remove(vertexName);
	}
	
	/**
	 *  Removes all locations from this
	 */	
	public void clear() {
		locs.clear();
	}
	
	public int size() {
		return locs.size();
	}
	
	/**
	 * 
	 * @return Returns the set of all vertexName, for which there is a location stored.
	 */
	public Set<String> getVertexNameSet() {
		return locs.keySet();
	}
	
}
