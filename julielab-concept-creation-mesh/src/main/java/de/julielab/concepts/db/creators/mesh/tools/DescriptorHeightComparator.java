package de.julielab.concepts.db.creators.mesh.tools;

import java.util.Comparator;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;

/**
 * Comparator to compare two descriptors by its heights.
 * 
 * @author Philipp Lucas
 * 
 */
public class DescriptorHeightComparator implements Comparator<Descriptor> {

	private Tree tree;

	public DescriptorHeightComparator(Tree tree) {
		this.tree = tree;
	}

	/**
	 * Compares <code>d1</code> and <code>d2</code> by theirs heights.
	 * 
	 * @return Returns <code>0</code> if <code>tree.heightOf(d1) == tree.heightOf(d2)</code>, <code>-1</code> if
	 *         <code>tree.heightOf(d1) < tree.heightOf(d2)</code>, and otherwise <code>1</code>.
	 */
	@Override
	public int compare(Descriptor d1, Descriptor d2) {
		int h1 = tree.heightOf(d1);
		if (h1 == -1)
			h1 = Integer.MAX_VALUE;
		int h2 = tree.heightOf(d2);
		if (h2 == -1)
			h2 = Integer.MAX_VALUE;
		if (h1 == h2) {
			return 0;
		} else if (h1 < h2)
			return -1;
		else {
			return 1;
		}
	}

}
