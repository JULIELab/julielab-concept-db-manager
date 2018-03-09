package de.julielab.concepts.db.creators.mesh;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;
import de.julielab.concepts.db.creators.mesh.tools.VertexComparator;

/**
 * <p>
 * This class is for determining which modification have been done to
 * <code>target</code> in order to create it from <code>source</code>.
 * 
 * <p>
 * Usage is the following:
 * <ol>
 * <li>Create an instance of <code>TreeComparatorUD</code> with appropriate
 * parameters.</li>
 * <li>Call <code>determineAddsAndMovings()</code> and
 * <code>determineDeletions</code>.</li>>
 * <li>Use <code>saveModificationsToFiles</code> to save all modifications to
 * files</li>>
 * or
 * <li>Use the <code>get****()</code> methods to retrieve the modifications
 * directly.
 * </ol>
 * </p>
 * 
 * <p>
 * The task accomplished here can be split into two parts:
 * <ol>
 * <li>Determining the original tree-numbers of all tree-vertices in
 * <code>target</code></li>
 * <li>Determining where they come from -> modifications</li>
 * </ol>
 * The restored tree-numbers will in fact be stored in <code>target</code> -
 * replacing the old tree-numbers.
 * </p>
 * 
 * <p>
 * This may seem confusing: we only save the modifications but not the correct
 * 'renaming'. Why? <br>
 * At this point we should remember that in MeSH tree-numbers have structural
 * meaning. Here, in contrast, they have not. They are just a IDs to identify
 * tree-vertices and we don't care about their actual value.
 * </p>
 * 
 * <p>
 * Notes: This class is NOT designed to determine the difference / the
 * modifications between arbitrary instances of <code>Tree</code>! Much more, it is
 * explicitly made for the "UD MeSH", i.e the old intermediate format of the
 * MeSH which was then used for import to semedico DBMS. Therefore:
 * <ul>
 * <li><code>target</code> should be created by <code>Parser4UserDefMesh</code>.</li>
 * <li>there are only three types of modifications: </li>
 * <ul>
 * <li>Descriptor additions (<code>getDescAdditions()</code>)</li>
 * <li>Vertex movings (<code>getVertexMovings()</code>)</li>
 * <li>Vertex deletions (<code>getVertexDeletions()</code>)</li>
 * </ul>
 * </p>
 * 
 * @author Philipp Lucas
 */

public class TreeComparatorUD extends TreeComparator {

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(TreeComparatorUD.class);

	// is set to true once the original tree-numbers / vertex-names have been
	// restored
	private boolean hasRestoredTreeNumbers = false;
	
	/**
	 * Standard constructor.
	 * 
	 * @param source
	 *            Should be complete original MeSH data.
	 * @param target
	 *            Parsed user-defined "old" MeSH as parsed with
	 *            <code>Parser4UserDefMesh</code>. <code>target</code> WILL BE
	 *            MODIFIED on detecting modifications!!
	 */
	public TreeComparatorUD(Tree source, Tree target) {
		super(source, target);
	}
	
	/**
	 * like the other constructor. but we can give a name to the comparator.
	 */
	public TreeComparatorUD(Tree source, Tree target, String name) {
		super(source, target, name);
	}
	
	/**
	 * Determines all modifications that made <code>source</code> to
	 * <code>target</code>, except for deletions. After calling this method the
	 * modifications can be retrieved using the methods
	 * <code>getNewDescs()</code>, <code>getMovedVertex2newParent()</code> and
	 * <code>getMovedVertex2origParent()</code>.
	 */
	@Override
	public void determineAddsAndMovings() {
		logger.info("# Starting to traverse UD-MeSH data to restore all tree-numbers... ");
		counter = new ProgressCounter(target.vertexSet().size(), 10,
				"tree-vertex");
		traverseForRestore(target.getRootVertex());
		counter.finishMsg();
		logger.info("# ... done restoring tree-number");
		hasRestoredTreeNumbers = true;
	}

	/**
	 * <p>
	 * Determines which tree-vertices have been deleted from original MeSH in
	 * order to create UD-MeSH. The findings are stored in some internal
	 * variables ( <code>vertexDeletions</code>, <code>delDescs</code> and
	 * <code>fullydelDescUis</code>). After calling this method the results can
	 * be retrieved using <code>getDelVertices()</code>,
	 * <code>getDelDescs()</code> and <code>getFullyDelDescUis()</code>.
	 * </p>
	 * Note: you must call <code>determineAddsAndMovings()</code> before calling
	 * this method in order to make it work. This is because
	 * <code>determineAddsAndMovings()</code> also restores the original
	 * vertex-names (->tree-numbers) which are needed here.
	 */
	@Override
	public void determineDeletions() {
		logger.info("# Checking for deleted vertices and descriptors ... ");
		counter = new ProgressCounter(source.vertexSet().size(), 10,
				"tree-vertex");

		if (!hasRestoredTreeNumbers) {
			logger.error("Original Tree-Number not yet restored - aborting! - call determineAddsAndMovings() before");
			return;
		}

		// store deleted vertices
		for (TreeVertex v : source.vertexSet()) {
			if (!target.hasVertex(v.getName())) {
				vertexDeletions.put(v.getName(), false);
			}
			counter.inc();
		}
		counter.finishMsg();
		
		// determine which deletions are fully recursive in fact
		vertexDeletions.detectRecursiveDeletions(source);
		
		// determine deleted descriptors
		// since there is no descriptor renaming it is easy: all those descriptors which are in source but not in target 
		for (Descriptor desc : source.getAllDescriptors()) {
			if (!target.hasDescriptorByUi(desc.getUI())) {
				descDeletions.add(desc.getUI());
			}
		}

		logger.info("# ... done.");
	}

	/**
	 * Recursively traverses <code>target</code> depth-first.
	 * 
	 * @param cur
	 *            Currently traversed tree-vertex.
	 */
	private void traverseForRestore(TreeVertex cur) {
		for (TreeVertex child : target.childVerticesOf(cur)) {
			TreeVertex origChild = restore(cur, child);
			counter.inc();
			traverseForRestore(origChild);
		}
	}

	/**
	 * It restores the original tree-number of <code>vertex</code> and also
	 * determines the modification which was applied to <code>vertex</code>.
	 * <p>
	 * Restores, for a given parent-child pair, the original position of child.
	 * No new tree-vertices will be added to UD-MeSH. Instead the existing
	 * tree-vertices in UD-MeSH will be <i>renamed</i> to their correct
	 * 'original name / tree-number'. While restoring the original position it
	 * is also determined which modification has been applied.
	 * </p>
	 * <p>
	 * <code>parent</code> is the "reference point" of this method as the
	 * parent-vertex is fixed and assumed (there is a exception - see below) to
	 * be valid already. 'Valid' means that its vertex-name is a valid
	 * vertex-name also in <code>source</code>, i.e. first of all that is not an
	 * artificial vertex. <br>
	 * There is one exception: when <code>parent</code> is a tree-vertex of a
	 * new descriptor obviously there is no corresponding counterpart in the
	 * original mesh. these new tree-vertices are assumed to bear a name
	 * starting with "art" or "facet " (including the traspace). then, these
	 * tree-vertices are allowed as well. <br>
	 * <code>parent</code> being such a 'reference point' is also the reason why
	 * it is necessary to return the original tree-number of <code>vertex</code>
	 * . We need it for calling <code>restore</code> for the children of
	 * <code>vertex</code>.
	 * </p>
	 * <p>
	 * When determining the origin there are three cases to examine:
	 * <ol>
	 * <li>child is not part of the original MeSH</li>
	 * <li>child is a child of parent in the original MeSH. What is it original
	 * tree-number?</li>
	 * <li>child is no such child. Where did it come from then?</li>
	 * </ol>
	 * Examining these cases for all tree-vertices of the UD-MeSH will
	 * eventually lead to a set of operations that have been applied on the
	 * original MeSH in order to produce the UD-MeSH.
	 * </p>
	 * <p>
	 * Notes:
	 * <ul>
	 * <li>for case 2: According to the results of
	 * <code>Various.testForParentVertexChildDescriptorPairUnambiguousness</code>
	 * it should be possible to determine uniquely and correctly the original
	 * tree-number (= vertex-name) of <code>child</code>.</li>
	 * </ul>
	 * </p>
	 * 
	 * @param parent
	 *            Parent tree-vertex of <code>child</code> (in UD-MeSH).
	 * @param vertex
	 *            A tree vertex of UD-MeSH.
	 * @return Returns the restored tree-vertex, with the original vertex-name /
	 *         tree-number.
	 */
	private TreeVertex restore(TreeVertex parent, TreeVertex vertex) {
		String childDescUi = vertex.getDescUi();
		Descriptor childDesc = target.getDescriptorByVertex(vertex);

		// pre-processing: sometimes even the descriptorUI of data in target got
		// changed too -> try to restore it by looking at the name of the
		// descriptor/term
		// note: normal descriptor UIs start with D
		if (!childDescUi.startsWith("D")) {
			// rename to correct UI
			Descriptor tmpDesc = source.getDescriptorByName(childDesc.getName());
			if (tmpDesc != null) {
				target.changeUiOf(childDesc, tmpDesc.getUI());
				childDescUi = vertex.getDescUi();
			}
		}

		/*
		 * case 1: child's descriptor was not a part of source.  
		 * -> new descriptor with new vertex
		 */
		if (!source.hasDescriptorByUi(childDescUi)) {
			descAdditions.addLocation(childDesc, vertex.getName(), parent.getName());
			return vertex;
		}

		/*
		 * case 2: child was a child-vertex of parent in the original MeSH -> no
		 * modifications here, just need to find the correct name of it
		 */
		// special case: parent is a tree vertex of a new descriptor -> case 2
		// cannot be applied.
		if ( !(parent.getName().startsWith("art") || parent.getName().startsWith("Facet ")) ) {
			TreeVertex parentInOrig = source.getVertex(parent.getName());
			for (TreeVertex ch : source.childVerticesOf(parentInOrig)) {
				if (ch.getDescUi().equals(childDescUi)) {
					// found it! -> change name of vertex in ud-mesh to the correct name
					target.renameVertex(vertex, ch.getName());
					return vertex;
				}
			}
		}

		/*
		 * case 3: then child must have been moved to here -> let's try to find
		 * out where it came from! it must be one of the tree-vertices of
		 * childDesc in the origMeSH - but which one?? idea: let's compare the
		 * offspring of child with the offspring of childs possible origins.
		 */
		TreeVertex origin = VertexComparator.getMostSimilarVertex(vertex, source.getDescriptorByUi(vertex.getDescUi()), target, source);
		TreeVertex parentInOrig = source.parentVertexOf(origin);

		// special case: it might happen, that this vertex was already imported as part of another file. In that case, don't add another one.
		if (target.hasVertex(origin.getName())) {
			// TODO: I believe we don't need to do anything with it, but just
			// return the correct name. Reason being that we want to infer
			// modifications to recreate the imported ud-mesh, but not the
			// ud-mesh xml files. 
			logger.warn( "Found duplicate tree vertex on import - I will not import the tree vertex a second time, but delete the duplicate instead!");
			logger.warn( "Duplicate tree vertex was : " + vertex.toString());

			// move all children of vertex to its copy (which we will keep)
			TreeVertex vertexKept = target.getVertex(origin.getName());
			for (TreeVertex child : target.childVerticesOf(vertex)) {
				target.moveBranch(child, vertexKept);
			}			
			target.cutVertex(vertex);
			return vertexKept;
		}
		
		// store findings
		vertexMovings.put(origin.getName(), parentInOrig.getName(), parent.getName(), origin.getDescUi(), vertex.getDescUi());
		
		//DEBUG:
		if ( !origin.getDescUi().equals(vertex.getDescUi()) ) {
			logger.error("Determining movement ud-mesh: desc-ui changed! original: " + origin.getDescUi() + " --- new: " + vertex.getDescUi() );
			System.exit(1);
		}

		// rename to correct name
		target.renameVertex(vertex, origin.getName());
		return vertex;
	}

	@Override
	public void determineRenamingsAndRebindings() {
		// logger.warn("determineRenamingsAndRebindings is not implemented." +
		// "This is because it is not needed.");
		// TODO: well.. in fact there are some renamings. see the TreeComparator
		// output if you run TestTreeComparatorUD which leads to the failing of
		// the test case.  
	}

}
