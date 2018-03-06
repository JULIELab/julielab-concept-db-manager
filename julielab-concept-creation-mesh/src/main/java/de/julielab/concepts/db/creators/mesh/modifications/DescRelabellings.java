package de.julielab.concepts.db.creators.mesh.modifications;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * Class representing Descriptor Renamings.
 *
 * <p>
 * NOTE: this is about renaming a descriptor to another descriptor name not another UI.
 * </p>
 * <p>
 * There is need to track descriptor name changes since we actually identify
 * descriptor by 2 primary keys: name and UI.
 * </p>
 * 
 * <p>
 * A renaming is a pair consisting of:
 * <ul>
 * <li>key = the old name of the descriptor</li>
 * <li>value = the new name of the descriptor</li>
 * </ul>
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */

public class DescRelabellings extends Renamings<String, String> {

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(DescRelabellings.class);

	/**
	 * Applies the descriptor name renamings  to <code>data</code>.
	 * 
	 * @param data
	 *            A <code>Tree</code> object.
	 */
	@Override
	public void apply(Tree data) {
		logger.info("# Renaming descriptor names in " + data.getName() + " ...");
		ProgressCounter counter = new ProgressCounter(size(), 10, "desc name renamings");
		for (String descName : getOldSet()) {
			data.changeNameOf(data.getDescriptorByName(descName), getNew(descName));
			counter.inc();
		}
		logger.info("# Done.");
	}

}