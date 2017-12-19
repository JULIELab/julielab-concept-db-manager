package de.julielab.semedico.mesh.modifications;

import org.slf4j.Logger;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.tools.ProgressCounter;

/**
 * <p>
 * Class representing Descriptor renamings.
 * </p>
 * <p>
 * NOTE: this is about renaming a descriptor to another descriptor UI not
 * another preferred name.
 * </p>
 * 
 * <p>
 * A renaming is a pair consisting of:
 * <ul>
 * <li>key = the old UI of the descriptor</li>
 * <li>value = the new UI of the descriptor</li>
 * </ul>
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */
public class DescRenamings
extends Renamings<String,String>{

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(DescRenamings.class);

	/**
	 * Applies the descriptor renamings to <code>data</code>.
	 * 
	 * @param data
	 *            A <code>Tree</code> object.
	 */
	public void apply(Tree data) {
		logger.info("# Renaming descriptors in " + data.getName() + " ...");
		ProgressCounter counter = new ProgressCounter(size(), 10, "desc UI renamings");
		for (String descUi : getOldSet()) {
			data.changeUiOf(data.getDescriptorByUi(descUi), getNew(descUi));
			counter.inc();
		}
		logger.info("# Done.");
	}
	
}
