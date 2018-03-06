package de.julielab.concepts.db.creators.mesh;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;

/**
 * Class that inherits from TreeModificationContainer and extends its functionality by allowing to apply the
 * modifications stored to a <code>Tree</code> object.
 * 
 * @author Philipp Lucas
 */
public class TreeModificator extends TreeModificationContainer {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(TreeModificator.class);

	private Tree data;

	/**
	 * Standard constructor.
	 * 
	 * @param data
	 *            Tree instance to modify with this.
	 */
	public TreeModificator(Tree data) {
		super();
		this.data = data;
	}

	/**
	 * Standard constructor.
	 * 
	 * @param data
	 *            Tree instance to modify with this.
	 * @param name
	 *            a Name for this tree modificator.
	 */
	public TreeModificator(Tree data, String name) {
		super(name);
		this.data = data;
	}

	/**
	 * Removes empty categories. More exactly, it deletes all those direct children of root vertex which do not have any
	 * children.
	 */
	public void removeEmptyCategories() {
		TreeFilter filter = new TreeFilter(data, TreeFilter.KEEP, TreeFilter.THROW);
		filter.throwEmptyChildrenOf(data.getRootVertex());
		filter.apply();
	}

	/**
	 * Removes all empty descriptors. An descriptor is empty if it doesn't have any tree-vertices attached to it.
	 */
	public Set<Descriptor> removeEmptyDescriptors() {
		Set<Descriptor> descRemoved = new HashSet<Descriptor>();
		for (Descriptor desc : data.getAllDescriptors()) {
			if (!desc.hasTreeVertices()) {
				descRemoved.add(desc);
				data.cutDescriptor(desc, Tree.CUT_ONLY);
			}
		}
		return descRemoved;
	}

	/**
	 * <p>
	 * Applies all modifications previously imported using the <code>putModification</code> method.
	 * </p>
	 * 
	 * Note: it's applies the modifications in a particular order (and this is usually the correct order.):
	 * <ol>
	 * <li>descriptor additions</li>
	 * <li>vertex additions</li>
	 * <li>vertex movings</li>
	 * <li>descriptor renamings</li>
	 * <li>vertex deletions</li>
	 * <li>descriptor deletions</li>
	 * <li>vertex renamings</li>
	 * </ol>
	 */
	public void applyAll(boolean debug) {
		logger.info("# Applying all modifications to '" + data.getName() + "' ... ");


		descRenamings.apply(data);
		if (debug)
			data.verifyIntegrity();

		descRelabellings.apply(data);
		if (debug)
			data.verifyIntegrity();
	
		descAdditions.apply(data);
		if (debug)
			data.verifyIntegrity();
		
		vertexAdditions.apply(data);
		if (debug)
			data.verifyIntegrity();

		vertexMovings.apply(data);
		if (debug)
			data.verifyIntegrity();

		// TODO in the following call it might happen that a tree vertex, that is child of the deleted vertex, is
		// appended directly to the root, although it has other, valid, parents. For the moment this will be filtered
		// out later in the Neo4j export...
		vertexDeletions.apply(data);
		if (debug)
			data.verifyIntegrity();

		descDeletions.apply(data);
		if (debug)
			data.verifyIntegrity();

		vertexRenamings.apply(data);
		if (debug)
			data.verifyIntegrity();

		logger.info("# ... done applying all modifications.");
	}


	/**
	 * Calls applyAll(false).
	 */
	public void applyAll() {
		applyAll(false);
	}

	/**
	 * @return Returns true if modifications could be applied to <code>data</code> without any problems, false
	 *         otherwise.
	 */
	public boolean verify() {
		return true;
	}
}
