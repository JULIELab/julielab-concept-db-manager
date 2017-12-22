package de.julielab.concepts.db.creators.mesh.modifications;

import java.util.LinkedHashMap;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * This class handles vertex movings. A moving of a vertex has the following
 * general appearance: (see source code) <br>
 * 
 * tree-nr1 tree-nr2 ======> desc1 desc2
 * 
 * Where, tree-nrX is the tree-nr of a vertex and descX is the corresponding
 * descriptor it belongs to. Of course, all tree-nr1,tree-nr2, desc1, desc2 must
 * exist.
 * 
 * <p>
 * Then, 'general moving' can have three occurrences:
 * <ol>
 * <li>rebinding a tree-vertex to a new descriptor: <br>
 * tree-nr1 == tree-nr2 && desc1 != desc2</li>
 * <li>'pure' moving: <br>
 * tree-nr1 != tree-nr2 && desc1 == desc2</li>
 * <li>combination of 1 and 2 <br>
 * tree-nr1 != tree-nr2 && desc1 != desc2</li>
 * </ol>
 * </p>
 * 
 * <p>
 * Note: we call it 'general moving' since we are moving in two structures:
 * <ul>
 * <li>the tree-vertex tree
 * <li>the descriptor-graph
 * </ul>
 * </p>
 * 
 * <p>
 * Note: in MeSH tree-numbers have got structural semantics. Thus, changing the
 * tree-number equals changing the location. Here, however, tree-numbers are
 * just labels, thus we store structural change by means of old and new parent
 * vertex name.
 * </p>
 * <p>
 * Note: In fact it would be very disadvantageous to store the new name of a
 * vertex as part of the movement. The reason is as follows: When applying
 * modifications, and movings in particular, the modifications might interfere
 * with each other. Say we move a vertex <code>a</code> to another location and
 * then we move a vertex <code>b</code> into the branch rooting in
 * </code>a</code>. If we would change the vertex name(s) of <code>a</code> and
 * its offspring, how could we identify the target of the moving of
 * <code>b</code>? In general, it appear simpler to keep all vertex-names
 * (tree-numbers) unchanged until all other modifications are applied.
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */
public class VertexMovings 
implements TreeModficationsInterface {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(VertexMovings.class);
	
	/*
	 * moved tree-vertices. It contains a quadruple of mappings for each (direct)
	 * moving of a tree-vertex. A moving is not direct when one if its ancestors
	 * was moved and thus it got moved as well.
	 */
	/*
	 * The key is always the name of the moved vertex, whiles the original
	 * parents name (<code>movedVertex2oldParent</code>), the new parents name
	 * (<code>movedVertex2newParent</code>), the new descriptor UI
	 * (<code>movedVertex2oldDescUi</code>), the old descriptor UI
	 * (<code>movedVertex2newDescUi</code>), respectively.
	 */
	private LinkedHashMap<String, String> movedVertex2newParent = new LinkedHashMap<String, String>();
	private LinkedHashMap<String, String> movedVertex2oldParent = new LinkedHashMap<String, String>();
	private LinkedHashMap<String, String> movedVertex2newDescUi = new LinkedHashMap<String, String>();
	private LinkedHashMap<String, String> movedVertex2oldDescUi = new LinkedHashMap<String, String>();
	
	public void put(String vertexName, String oldParentVertexName, String newParentVertexName, String oldDescUi, String newDescUi) {
		boolean overwrite = false;
		if(contains(vertexName)) {
			logger.warn("Overwriting existing vertex moving!");
			logger.warn("existing = " + toString(vertexName));
			overwrite = true;
		}
		
		movedVertex2newParent.put(vertexName, newParentVertexName);
		movedVertex2oldParent.put(vertexName, oldParentVertexName);
		movedVertex2newDescUi.put(vertexName, newDescUi);
		movedVertex2oldDescUi.put(vertexName, oldDescUi);
		
		if(overwrite) {
			logger.warn("new      = " + toString(vertexName));
		}
	}
	
	public String getNewParent(String vertexName) {
		return movedVertex2newParent.get(vertexName);
	}
	
	public String getOldParent(String vertexName) {
		return movedVertex2oldParent.get(vertexName);
	}	
	
	public String getNewDescUi(String vertexName) {
		return movedVertex2newDescUi.get(vertexName);
	}
	
	public String getOldDescUi(String vertexName) {
		return movedVertex2oldDescUi.get(vertexName);
	}
	
	
	public void apply(Tree data) {
		logger.info("# Moving vertices in " + data.getName() + " ...");
		ProgressCounter counter = new ProgressCounter(movedVertex2newParent.size(), 10, "vertex");
	
		for(String vertexName : movedVertex2newParent.keySet()) {
			// descriptor rebinding
			String newDescUI = getNewDescUi(vertexName);
			String oldDescUI = getOldDescUi(vertexName);
			if(!newDescUI.equals(oldDescUI)) {
				data.changeDescOf(data.getVertex(vertexName), data.getDescriptorByUi(newDescUI));
			}
			
			// tree-vertex moving
			String newParent = movedVertex2newParent.get(vertexName);
			String oldParent = movedVertex2oldParent.get(vertexName);
			if(!newParent.equals(oldParent)) {
				data.moveBranch(vertexName, newParent);

			}
			counter.inc();
		}
		logger.info("# Done.");
	}
	
	/**
	 * @param vertexName A vertex Name
	 * @return Returns <code>true</code> if a vertex moving of a vertex with name
	 * <code>vertexName</code> exists. <code>false</code> otherwise.
	 */
	public boolean contains(String vertexName) {
		return movedVertex2newDescUi.containsKey(vertexName);
	}

	public Set<String> keySet() {
		return movedVertex2newParent.keySet();
	}
	
	public int size() {
		return movedVertex2newParent.size();
	}
	
	public String toString(String movedVertex) {
		return  "old name : " + movedVertex + " --- " +
				"old parent : " + getOldParent(movedVertex) + " --- " +
				"new parent : " + getNewParent(movedVertex) + " --- " +
				"old descUI : " + getOldDescUi(movedVertex) + " --- " +
				"new descUI : " + getNewDescUi(movedVertex);
	}
	
	@Override
	public String toString() {
		StringBuffer out = new StringBuffer();
		for(String v : keySet()) {
			out.append(toString(v) + "\n");
		}
		return out.toString();
	}
	
	@Override
	public boolean isEmpty() {
		return movedVertex2newDescUi.isEmpty();
	}
	
//	/**
//	 * @return Returns a map containing for each moved vertex the name of the
//	 *         new parent (i.e. the one in <code>udMesh<code>.
//	 * Note that the parent vertex name is the restored name already!
//	 */
//	public Map<String, String> getMovedVertex2newParent() {
//		return movedVertex2newParent;
//	}
//
//	/**
//	 * @return Returns a map containing for each vertex(name) that got moved the
//	 *         original parent (i.e. the one in <code>origMesh<code>.
//	 */
//	public Map<String, String> getMovedVertex2origParent() {
//		return movedVertex2origParent;
//	}
}
