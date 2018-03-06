package de.julielab.concepts.db.creators.mesh;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * <p>This class allows to filter (remove) unwanted vertices of a given Tree
 * instance. Note that it NEVER removes any descriptors itself, but only their 
 * vertices, even if a descriptor has no vertices left.</p>
 * 
 * It works in a 4-step process:
 * <ol>
 * <li>Create an instance of TreeFilter by providing the tree object to modify,
 * as well as the default for all nodes.</li>
 * <li>Use provided methods to mask vertices / descriptors as 'keep' or 'throw'</li>
 * <li>Call apply() to apply actual changes to the tree.</li>
 * <li>optional: call removeEmptyDesc() to remove all those descriptors without any vertex.
 * </ol>
 * 
 * <p>In the documentation of the methods below 'keep' always means that a certain
 * tree-vertex is masked to be kept, thus will not be removed when apply() is
 * called. 'throw' means that it will be removed when apply() is called.</p>
 * 
 * <p>The advantage of using this class rather than <code>Tree.cutVertex</code> and
 * <code>cutBranch</code> is that it will never leave the Tree disconnected. See
 * <code>apply()</code> for details.</p>
 * 
 * <p>Also, using this class is much more efficient when filtering a large number 
 * Tree Vertices.</p>
 *
 * @author Philipp Lucas
 */
public class TreeFilter {
	
	public static final boolean KEEP = true;
	public static final boolean THROW = false;

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(TreeFilter.class);

	// tree instance to modify
	private Tree tree;

	// default to keep = true or throw = false 
	private boolean def;
	
	// true 
	private boolean keepEmptyDescs;

	// to store the masking
	private HashSet<TreeVertex> mask = new HashSet<TreeVertex>();

	/**
	 * Constructor.
	 * @param tree Tree object to alter.
	 * @param dfault Default for keeping or throwing. Pass either <code>TreeFilter.KEEP</code> or <code>TreeFilter.THROW</code>. 
	 * @param keepEmptyDescs Remove empty descriptors when applying the filter, or not? Pass either <code>TreeFilter.KEEP</code> or <code>TreeFilter.THROW</code>.
	 */
	public TreeFilter(Tree tree, boolean dfault, boolean keepEmptyDescs) {
		this.tree = tree;
		this.def = dfault;
		this.keepEmptyDescs = keepEmptyDescs;
		maskAll();
	}

	/**
	 * Resets the mask to the default.
	 */
	private void maskAll() {
		mask.clear();

		// always keep the root!
		if (def == THROW) {
			mask.add(tree.getRootVertex());
		}		
	}

//	/**
//	 * Masks all vertices whose names is given in <code>vertexNames</code> to be
//	 * kept.
//	 * 
//	 * @param vertexNames
//	 *            A list of vertex names.
//	 * @param recDepth
//	 *            Set 'how deep' the operation is applied. Set to 1 to keep only
//	 *            the vertices in <code>vertexNames</code>. Set to 2 to keep
//	 *            also the children of them. And so on... If you set it to -1 it
//	 *            will be fully recursive.
//	 */
//	public void keepTreeVertices(List<String> vertexNames, int recDepth) {
//		setTreeVertices(vertexNames, true, recDepth);
//	}
//
//	/**
//	 * Masks all vertices whose names is given in <code>vertexNames</code> to be
//	 * thrown.
//	 * 
//	 * @param vertexNames
//	 *            A list of vertex names.
//	 * @param recDepth
//	 *            Set 'how deep' the operation is applied. Set to 1 to keep only
//	 *            the vertices in <code>vertexNames</code>. Set to 2 to keep
//	 *            also the children of them. And so on... If you set it to -1 it
//	 *            will be fully recursive.
//	 */
//	public void throwTreeVertices(List<String> vertexNames, int recDepth) {
//		setTreeVertices(vertexNames, false, recDepth);
//	}

	/**
	 * Marks all all vertices whose names is given in <code>vertexNames</code>
	 * to be kept or thrown according to <code>flag</code>. <code>flag</code> ==
	 * true -> keep, <code>flag</code> == false -> throw.
	 * 
	 * @param vertexNames
	 *            A list of vertex names.
	 * @param flag
	 *            Set to true if vertices should be kept, and to false if they
	 *            should be thrown.
	 * @param recDepth
	 *            Set 'how deep' the operation is applied. Set to 1 to keep only
	 *            the vertices in <code>vertexNames</code>. Set to 2 to keep
	 *            also the children of them. And so on... If you set it to -1 it
	 *            will be fully recursive.
	 */
	public void maskTreeVertices(Set<String> vertexNames, boolean flag,
			int recDepth) {
		for (String name : vertexNames) {
			maskTreeVertex(tree.getVertex(name), flag, recDepth);
		}
	}

	/**
	 * Masks tree vertex <code>vertex</code> to be kept (<code>flag</code> ==
	 * KEEP) or thrown (<code>flag</code> == THROW).
	 * 
	 * @param vertex
	 *            A tree vertex.
	 * @param flag
	 *            Set to true if vertex should be kept, and to false if it
	 *            should be thrown.
	 * @param recDepth
	 *            Set 'how deep' the operation is applied. Set to 1 to keep/throw only
	 *            the vertex <code>vertex</code>. Set to 2 to keep/throw
	 *            also the children of them. And so on... If you set it to -1 it
	 *            will be fully recursive.
	 */
	public void maskTreeVertex(TreeVertex vertex, boolean flag,
			int recDepth) {
		
		if (flag != def) {
			mask.add(vertex);
		}
		
		if (recDepth != 0) {
			for (TreeVertex child : tree.childVerticesOf(vertex)) {
				maskTreeVertex(child, flag, recDepth-1);
			}
		}
	}

	/**
	 * Masks a list of descriptors according to flag, i.e. all terms (= tree
	 * vertices) of this descriptor will be masked. Can also be done
	 * recursively, which will cause all descriptors which have a term that is a
	 * offspring of a term of a descriptor in names to be masked as well. Again,
	 * note that this works not on term-basis but descriptor-basis!
	 * 
	 * @param names
	 *            List of names of descriptors.
	 * @param flag
	 *            Set to true if descriptors should be kept, and to false if
	 *            they should be thrown.
	 * @param recursive
	 *            If set to true, mask will be applied recursively.
	 */
	public void maskDescByNameList(List<String> names, boolean flag,
			boolean recursive) {
		for (String s : names) {
			Descriptor desc = tree.getDescriptorByName(s);
			maskDesc(desc, flag, recursive);
		}
	}

	/**
	 * Masks a list of descriptors according to flag, i.e. all terms (= tree
	 * vertices) of this descriptor will be masked. Can also be done
	 * recursively, which will cause all descriptors which have term that is a
	 * offspring of a term of a descriptor in uis to be masked as well. Again,
	 * note that this works not on term-basis but descriptor-basis.!
	 * 
	 * @param names
	 *            List of DescriptorUIs of descriptors.
	 * @param flag
	 *            Set to true if descriptors should be kept, and to false if
	 *            they should be thrown.
	 * @param recursive
	 *            If set to true, mask will be applied recursively.
	 */
	public void maskDescByUiList(List<String> uis, boolean flag,
			boolean recursive) {
		for (String s : uis) {
			Descriptor desc = tree.getDescriptorByUi(s);
			if (desc == null) {
				logger.warn("unknown DescriptorUI '" + s + ". I ignored it.");
				continue;
			}
			maskDesc(desc, flag, recursive);
		}
	}
	

	/**
	 * Masks the descriptor desc according to flat to be kept or thrown. Can be
	 * done recursively.
	 * 
	 * @param desc
	 *            A descriptor.
	 * @param flag
	 *            true to keep, false to throw.
	 * @param recursive
	 *            true to apply it recursively, false for not.
	 */
	public void maskDesc(Descriptor desc, boolean flag, boolean recursive) {
		// mask all tree vertices of the given descriptor
		for (TreeVertex v : desc.getTreeVertices()) {
			
			if (flag != def) {
				mask.add(v);
			}

			// and, if recursive flag is set ...
			if (recursive) {
				// do the same for all descriptors that got a child of the
				// 'current' tree vertex v.
				for (TreeVertex child : tree.childVerticesOf(v)) {
					Descriptor childDesc = tree.getDescriptorByUi(child
							.getDescUi());
					maskDesc(childDesc, flag, recursive);
				}
			}
		}
	}

	/**
	 * Masks all descriptors with UIs in file-path and potentially also their
	 * offsprings. The format is assumed to be plain text with a single
	 * DescriptorUI on each line.
	 * 
	 * @param filepath
	 *            The file path to the file that contains the UIs
	 * @param flag
	 *            true for a white-list, false for a black-list
	 * @param recursive
	 *            true to apply it recursively, false for not.
	 */
	public void maskDescByUIFile(String filepath, boolean flag,
			boolean recursive) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(filepath));
			ProgressCounter counter = new ProgressCounter(0, 1000, "filter-UI");
			while (in.ready()) {
				String descUi = in.readLine().trim();
				Descriptor desc = tree.getDescriptorByUi(descUi);
				if (desc == null) {
					logger.warn("unknown DescriptorUI '" + descUi
							+ "'. I ignored it.");
				} else {
					maskDesc(desc, flag, recursive);
				}
				counter.inc();
			}
			in.close();
			counter.finishMsg();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * <p>
	 * Applies the changes according to the masking done before. Note that it
	 * also resets the masking of all vertices to the default.
	 * </p>
	 * <p>
	 * As a result of this method the Tree that is worked on will not become
	 * disconnected, even if non-leave vertices are removed. This is achieved by
	 * 'sliding' the otherwise disconnected vertices up towards the root till we
	 * get to an ancestor of it that is not removed.
	 * </p>
	 * <p>
	 * However, note that this has a time complexity of
	 * O(nr-of-vertices-in-tree). Thus, try to gather all filter action before
	 * applying them!
	 * </p>
	 * 
	 */
	public void apply() {
		// apply changes
		traverseAndMove(tree.getRootVertex(), tree.getRootVertex());
		traverseAndClean(tree.getRootVertex());

		// reset mask
		maskAll();
	}

	/**
	 * Moves all vertices that are kept to their new position. This is done by
	 * traversing the tree depth-first. At any time the closest ancestor vertex
	 * (that will be kept) of the currently traversed vertex is known. Their
	 * names are <code>closestAnc</code> and <code>cur</code>, resp. Thus, when
	 * <code>cur</code> should also be kept, then its new parent should be
	 * <code>clostestAnc</code>, thus we move the branch which roots in
	 * <code>cur</code> to <code>clostestAnc</code>. If <code>cur</code> should
	 * not be kept, it can be removed.
	 * 
	 * @param closestAnc
	 *            Closest living ancestor tree vertex of <code>cur</code.
	 * @param cur
	 *            Currently traversed tree vertex.
	 */
	private void traverseAndMove(TreeVertex closestAnc, TreeVertex cur) {
		// move branch if necessary
	    if ( def && !mask.contains(cur) || !def && mask.contains(cur) ) {
			if (!tree.isParentVertex(closestAnc, cur)) {
				tree.moveBranch(cur, closestAnc);
			}
			closestAnc = cur;
		}
		// traverse down ...
		for (TreeVertex child : tree.childVerticesOf(cur)) {
			traverseAndMove(closestAnc, child);
		}
	}

	/**
	 * Removes all vertices and edges that are not kept. Deletes all children of
	 * a vertex before it deletes a vertex itself.
	 * 
	 * @param cur
	 *            Currently traversed tree vertex.
	 */
	private void traverseAndClean(TreeVertex cur) {
		for (TreeVertex child : tree.childVerticesOf(cur)) {
			traverseAndClean(child);
		}

		if ( def && mask.contains(cur) || !def && !mask.contains(cur) ) {
			Descriptor d = tree.getDescriptorByVertex(cur);
			tree.cutVertex(cur);
			
			if (keepEmptyDescs == TreeFilter.THROW 
				&& !d.hasTreeVertices()) {
				tree.cutDescriptor(d, Tree.CUT_ONLY);
			}			
		}
	}

	/**
	 * <p>Throw all those children of <code>vertex</code> which don't have any
	 * children (and not just no children that will be kept!).</>p>
	 * 
	 * Note: this method must only be called when the default is to keep vertices!
	 * 
	 * @param A
	 *            tree vertex.
	 */
	public void throwEmptyChildrenOf(TreeVertex vertex) {
		if (def == KEEP) {
			for (TreeVertex v : tree.childVerticesOf(vertex)) {
				if (tree.childVerticesOf(v).size() == 0) {
					mask.add(v);
				}
			}
		}
	}
	
//	/**
//	 * Removes all those descriptors which do not have any tree vertex.
//	 */
//	public void cutEmptyDescs () {
//		for (Descriptor d : tree.getAllDescriptors()) {
//			if (!d.hasTreeVertices()) {
//				tree.cutDescriptor(d, Tree.CUT_ONLY);
//			}
//		}
//	}
	
}
