package de.julielab.concepts.db.creators.mesh.modifications;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * Class for handling a set of Vertex Renamings. Can be applied to a <code>Tree</code>-Object.
 *  
 * @author Philipp Lucas
 */

public class VertexRenamings 
extends Renamings<String,String> {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(VertexRenamings.class);
	
	/**
	 * Applies the vertex renamings to <code>data</code>.
	 * 
	 * @param data
	 *            A <code>Tree</code> object.
	 */
	@Override
	public void apply(Tree data) {
		logger.info("# Renaming vertices in " + data.getName() + " ...");
		ProgressCounter counter = new ProgressCounter(size(), 10, "vertex renamings");
		for (String nameOld : getOldSet()) {
			TreeVertex v = data.getVertex(nameOld);
			data.renameVertex(v, getNew(nameOld));
			counter.inc();
		}
		logger.info("# Done.");
	}
}
