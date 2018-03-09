package de.julielab.concepts.db.creators.mesh.modifications;

import de.julielab.concepts.db.creators.mesh.Tree;

/**
 * Interface for modification containers.
 * 
 * @author Philipp Lucas
 *
 */
public interface TreeModficationsInterface {
	public void apply(Tree data);
	public boolean isEmpty();
}
