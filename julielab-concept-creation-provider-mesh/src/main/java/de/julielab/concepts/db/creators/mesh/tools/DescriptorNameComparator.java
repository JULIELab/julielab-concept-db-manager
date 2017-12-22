package de.julielab.concepts.db.creators.mesh.tools;

import java.util.Comparator;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;

/**
 * Comparator to compare two descriptors by its names.
 * 
 * @author Philipp Lucas
 *
 */
public class DescriptorNameComparator implements Comparator<Descriptor> {
	/**
	 * 
	 * @param d1 A descriptor.
	 * @param d2 A descriptor.
	 * @return Returns d1.getName().compareTo(d2.getName());
	 */
	@Override
	public int compare(Descriptor d1, Descriptor d2) {		
		String name1 = d1.getName();
		String name2 = d2.getName();
		return name1.compareTo(name2);
	}

}
