package de.julielab.semedico.mesh.components;

import de.julielab.semedico.mesh.Tree;

/**
 * <p>
 * Each instance of TreeVertex is a node in a Tree instance. Thus, a node represents a descriptor-record at a certain
 * location in the MeSH-tree.
 * </p>
 * 
 * This is different from <code>TreeNumber</code>, as the latter one is only used for importing or exporting
 * TreeNumbers, but not for representing them in a <code>Tree</code> instance.
 * 
 * @author Philipp Lucas
 */
public class TreeVertex implements Comparable<TreeVertex> {
	// unique number of that node within it's siblings. It corresponds to a part
	// of the original tree-number.
	private String partialTreeNumber;
	private String name = null;

	// functions as a key for the hashmap that will direct to the full
	// descriptor record that's connected to a tree-vertex
	private String descName;

	// same as descName just for the descriptors UI
	private String descUi;

	// height of this tree vertex. If height == Integer.MIN_VALUE then it needs
	// to be updated.
	// Otherwise it represents the height of this vertex.
	private int height = Integer.MIN_VALUE;

	/**
	 * Copy constructor
	 * 
	 * @param v
	 *            Tree vertex to copy.
	 */
	public TreeVertex(TreeVertex v) {
		this.partialTreeNumber = v.partialTreeNumber;
		this.name = v.name;
		this.descName = v.descName;
		this.height = v.height;
	}

	/**
	 * Normal constructor.
	 * 
	 * @param partialTreeNumber
	 * @param name
	 * @param descName
	 */
	public TreeVertex(String partialTreeNumber, String name, String descName, String descUi) {
		this.partialTreeNumber = partialTreeNumber;
		this.setName(name);
		this.descName = descName;
		this.descUi = descUi;
	}

	public String getDescName() {
		return descName;
	}

	public void setDescName(String descName) {
		this.descName = descName;
	}

	public String getPartialTreeNumber() {
		return partialTreeNumber;
	}

	public void setPartialTreeNumber(String partialTreeNumber) {
		this.partialTreeNumber = partialTreeNumber;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	/**
	 * If it returns false, the heights needs to be recalculated. This is because height of a vertex is cached for
	 * performance reasons.
	 * 
	 * @return Returns true if the height retrieved by getHeight() is valid; false else.
	 */
	public boolean hasValidHeight() {
		return (height != Integer.MIN_VALUE);
	}

	/**
	 * Sets the height of this vertex to be invalid, i.e. that it needs to be recalculated.
	 */
	public void setHeightInvalid() {
		setHeight(Integer.MIN_VALUE);
	}

	/**
	 * @return Returns the name of this vertex. If a tree number was used to add this vertex, this will return the tree
	 *         number as it was at the time of adding it. However, the effective tree number might have changed since
	 *         then.
	 */
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "( " + descName + " :: " + descUi + " :: " + name + ")";
	}

	/**
	 * 
	 * @param data
	 *            The Tree that contains this tree vertex.
	 * @return Returns a full string representation of this tree vertex.
	 */
	public String toFullString(Tree data) {
		StringBuilder str = new StringBuilder();
		str.append(" Tree Number" + "\t" + data.treeNumberOf(this) + "\n");
		str.append(" Name \t" + getName() + "\n");
		str.append(" Height" + "\t" + data.heightOf(this) + "\n");
		str.append(" Belongs to descriptor " + data.getDescriptorByVertex(this) + "\n");
		return str.toString();
	}

	public String getDescUi() {
		return descUi;
	}

	public void setDescUi(String descUi) {
		this.descUi = descUi;
	}

	/**
	 * This comparison method is not particularly meaningful, it just serves to get SOME ordering to stabilize results
	 * of operations across several runs of the program with same parameters.
	 */
	@Override
	public int compareTo(TreeVertex o) {
		return (descUi + partialTreeNumber + height).compareTo(o.descUi + o.partialTreeNumber + o.height);
	}

}
