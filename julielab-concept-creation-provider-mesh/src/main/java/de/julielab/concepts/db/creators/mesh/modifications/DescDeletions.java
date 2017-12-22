package de.julielab.concepts.db.creators.mesh.modifications;

import java.util.LinkedHashSet;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.TreeFilter;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * <p>
 * Class that deals with descriptor deletions. It stores descriptor deletions by
 * storing the UI of the deletions. Thus, it does not hold a full copy of the
 * descriptors to be deleted.
 * </p>
 * <p>
 * Descriptor deletion means that the descriptor and all of its tree-vertices are removed.
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */
public class DescDeletions
extends LinkedHashSet<String> 
implements TreeModficationsInterface {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(DescDeletions.class);
	
	private static final long serialVersionUID = 1L;

	/**
	 * <p>
	 * Apply the descriptor deletions on <code>data</code>.
	 * </p>
	 * 
	 * <p>
	 * As a result of this all tree-vertices of the descriptors with UIs
	 * currently stored will be deleted. In addition, also the descriptor itself
	 * will be removed from <code>data</code>.
	 * </p>
	 * 
	 * @param data
	 */
	public void apply (Tree data) {
		logger.info("# Deleting descriptors from " + data.getName() + " ...");
		ProgressCounter counter = new ProgressCounter(size(), 10, "descriptor");
		
		TreeFilter filter = new TreeFilter(data, TreeFilter.KEEP, TreeFilter.THROW);
		for (String descUi : this) {
			if (data.hasDescriptorByUi(descUi)) {
				filter.maskDesc(data.getDescriptorByUi(descUi), TreeFilter.THROW, false);
			}
			counter.inc();
		}
		filter.apply();
		
		// TODO: Mir ist völlig unklar, warum das notwendig ist. Eigentlich
		// sollten alle "leeren" descriptoren bereits oben mit gelöscht werden,
		// da beim Erstellen des Filters als 3. Argument "TreeFilter.THROW"
		// angegeben wurde.
		for (String descUi : this) {
			Descriptor d = data.getDescriptorByUi(descUi);
			if (d != null) {
				data.cutDescriptor(d ,Tree.CUT_ONLY);
			}
		}
		logger.info("# Done.");
	}
	
	/**
	 * Returns a string representation of this.
	 */
	@Override
	public String toString() {
		StringBuffer out = new StringBuffer();
		for (String descUi : this) {
			out.append(descUi + "\n");
		}
		return out.toString();
	}
	

	/** 
	 * Inserts <code>descUi</code> to this set, if not already there.
	 * Returns true iff successfully inserted, false otherwise.
	 */
	@Override
	public boolean add(String descUi) {
		if(super.add(descUi)) {
			return true;
		} else {
			logger.warn("Overwriting existing descriptor deletion : " + descUi);
			return false;
		}
	}
	
}
