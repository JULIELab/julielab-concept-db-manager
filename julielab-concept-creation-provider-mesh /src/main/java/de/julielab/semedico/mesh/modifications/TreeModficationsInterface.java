package de.julielab.semedico.mesh.modifications;

import de.julielab.semedico.mesh.Tree;

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
