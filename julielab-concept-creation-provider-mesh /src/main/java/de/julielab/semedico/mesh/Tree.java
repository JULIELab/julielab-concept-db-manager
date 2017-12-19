package de.julielab.semedico.mesh;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;

import de.julielab.semedico.mesh.components.Concept;
import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.Term;
import de.julielab.semedico.mesh.components.TreeNumber;
import de.julielab.semedico.mesh.components.TreeVertex;
import de.julielab.semedico.mesh.components.VertexLocations;
import de.julielab.semedico.mesh.tools.DescriptorHeightComparator;

/**
 * 
 * <p>
 * This is the central class, representing a MeSH-Tree and providing methods for both: to query and modify its data.
 * </p>
 * 
 * <p>
 * Represents a MeSH-Tree, i.e. a hierarchical set of data. Thus it is a tree, so that each node only has zero
 * (root-node) or one (all other nodes) parent. Edges always have their target at that node which got a greater distance
 * from the root-node then their source.
 * </p>
 * 
 * <p>
 * There are several ways to access data of it:
 * <ul>
 * <li>use the graph and methods on the graph itself. Note that you can get the root vertex by <code>getRoot()</code>.</li>
 * <li>get a descriptor by its descriptor name using <code>getDescriptorByName </code></li>
 * <li>get a descriptor by its descriptor UI using <code> getDescriptorByUi </code>
 * <li>get a tree vertex by it's original tree number using <code>getVertex</code></li>
 * <li>... and many more ...</li>
 * </ul>
 * </p>
 * 
 * <p>
 * However, misuse of the provided methods can very well result in a invalid tree, i.e. disconnected part of the tree,
 * etc. You can confirm that the present state of a <code>Tree</code>-Object is a Tree via the
 * <code>verifyIntegrity</code> method.
 * </p>
 * 
 * <p>
 * Some more notes about tree numbers:
 * <ul>
 * <li>the term <i> tree number </i> refers to the internal tree number of a tree vertex in this class. In fact these
 * tree numbers are not needed but are maintained for the sake of completeness. Also, they are calculated when required,
 * but not statically stored as a value.
 * <li>the term <i>original tree number </i> refers to the tree number of a tree vertex as in in the original MeSH XML
 * data. There is a hash map <code>vertexName2vertex</code>, which maps them to tree vertices. Thus original tree
 * numbers can be used to identify a tree vertex, even after it has been moved. In the end, <i>original tree numbers</i>
 * are just the name of a tree vertex - and thus its identifier.
 * </ul>
 * </p>
 * 
 * <p>
 * An instance of Tree always contains at least one descriptor and one tree vertex:
 * <ol>
 * <li>root descriptor: its UI is "root", its name is "root-node". It has exactly one tree node which is the root of the
 * tree. The original tree number of it is "root" and its partial tree-number also is "root".</li>
 * </ol>
 * </p>
 * 
 * @author Philipp Lucas
 */
public class Tree extends DefaultDirectedGraph<TreeVertex, DefaultEdge> {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(Tree.class);
	private static final long serialVersionUID = 1L;

	public static final int CUT_ONLY = 0;
	public static final int CUT_AND_SLIDEUP = 1;

	/**
	 * Another entry point to the data. Unlike the super class, this class provides a separate hash table to hold all
	 * real data. The graph itself just represents the hierarchical structure of the data hold in the hash table. Thus,
	 * vertices of the graph simply contain keys of the hash table (and their local part of the tree number).
	 */
	private LinkedHashMap<String, Descriptor> descName2desc = new LinkedHashMap<String, Descriptor>();

	/**
	 * Another entry point to the data. Maps a descriptor's UI
	 * (http://www.nlm.nih.gov/mesh/xml_data_elements.html#DescriptorUI) to a descriptor.
	 */
	private LinkedHashMap<String, Descriptor> descUi2desc = new LinkedHashMap<String, Descriptor>();

	/**
	 * only for addition / import - stores tree vertices that still need to be connected to their parents. This cannot
	 * always be done straight away, since they might not come in order. The key refers to the name of the parent
	 * vertex, while the value refers to the vertices which are waiting for the key.
	 */
	private LinkedHashMap<String, List<TreeVertex>> pendingVertices = new LinkedHashMap<String, List<TreeVertex>>();

	/**
	 * <p>
	 * A hash map that maps a name of a vertex to its vertex / tree-node. Depending on the data added, this might be
	 * original tree-numbers or just a chosen name. e.g. to root node has the name "root".
	 * </p>
	 * <p>
	 * Data is added to a Tree instance by giving the data to add, and also the location where to add it - and this
	 * location is to be specified by a name of a tree-vertex.
	 * </p>
	 */
	private LinkedHashMap<String, TreeVertex> vertexName2vertex = new LinkedHashMap<String, TreeVertex>();

	/** Root node of the tree */
	private TreeVertex root;
	private Descriptor rootDesc;

	// name of this tree object
	private String name;

	/**
	 * Constructor.
	 * 
	 * @param edgeClass
	 *            Class for the edges of this tree.
	 * @param name
	 *            Name of this tree.
	 */
	public Tree(Class<? extends DefaultEdge> edgeClass, String name) {
		super(edgeClass);
		addRoot();
		this.name = name;
		// addMaleFemale();
	}

	/**
	 * Constructor that defaults <code>edgeClass</code> to <code>DefaultEdge.class</code>.
	 * 
	 * @param name
	 *            Name of this tree.
	 */
	public Tree(String name) {
		super(DefaultEdge.class);
		addRoot();
		this.name = name;
		// addMaleFemale();
	}

	/**
	 * add root element to tree. root element is a descriptor with only one concept and one term. Its name is
	 * "root-node", its UI is "root" and it's partial tree-number is "root".
	 */
	private void addRoot() {
		rootDesc = new Descriptor();
		Concept c = new Concept(true);
		Term t = new Term("root-node", true);
		c.addTerm(t);
		rootDesc.addConcept(c);
		rootDesc.setUI("root");

		root = new TreeVertex("root", "root", rootDesc.getName(), "root");
		root.setHeight(0);
		rootDesc.addTreeVertex(root);

		descName2desc.put(rootDesc.getName(), rootDesc);
		descUi2desc.put(rootDesc.getUI(), rootDesc);
		vertexName2vertex.put(root.getName(), root);

		addVertex(root);
	}

	/**
	 * @param desc
	 *            A descriptor
	 * @return Returns true if <code>desc</code> equals the root descriptor.
	 */
	public boolean isRoot(Descriptor desc) {
		return desc.equals(rootDesc);
	}

	/**
	 * @param vertex
	 *            A tree vertex.
	 * @return Returns true if <code>vertex</code> equals the root vertex.
	 */
	public boolean isRoot(TreeVertex vertex) {
		return vertex.equals(root);
	}

	/**
	 * This will add the "Male" and "Female" descriptor to the tree:
	 * <ul>
	 * <li>male descriptor: its UI is "D008297", its name is "Male". It got one synonym: "Males". It doesn't have any
	 * tree vertices.</li>
	 * <li>female descriptor: its UI is "D005260", its name is "Male". It got one synonym: "Females". It doesn't have
	 * any tree vertices.</li>
	 * </ul>
	 */
	public void addMaleFemale() {
		Descriptor male = new Descriptor();
		Concept c = new Concept(true);
		Term t = new Term("Male", true);
		c.addTerm(t);
		t = new Term("Males", false);
		c.addTerm(t);
		male.addConcept(c);
		male.setUI("D008297");

		descName2desc.put(male.getName(), male);
		descUi2desc.put(male.getUI(), male);

		Descriptor female = new Descriptor();
		c = new Concept(true);
		t = new Term("Female", true);
		c.addTerm(t);
		t = new Term("Females", false);
		c.addTerm(t);
		female.addConcept(c);
		female.setUI("D005260");

		descName2desc.put(female.getName(), female);
		descUi2desc.put(female.getUI(), female);
	}

	/**
	 * A root node always exists, as it is created when created the instance of this class.
	 * 
	 * @return Returns the root vertex of this tree.
	 */
	public TreeVertex getRootVertex() {
		return this.root;
	}

	/**
	 * A root descriptor always exists, as it is created when created the instance of this class.
	 * 
	 * @return Returns the root descriptor of this tree.
	 */
	public Descriptor getRootDesc() {
		return this.rootDesc;
	}

	/**
	 * @return Returns the name of this <code>Tree</code> object.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Removes a branch that roots in vertex from the tree. If the given vertex doesn't exist there are no effects.
	 * Vertex itself is removed as well. If any of the corresponding descriptors of deleted vertices do not have any
	 * tree vertices left they are deleted as well.
	 * 
	 * @param vertex
	 *            Root vertex of branch to cut.
	 */
	public void cutBranch(TreeVertex vertex) {
		if (!containsVertex(vertex)) {
			return;
		}
		// call cut branch for all children
		for (TreeVertex v : childVerticesOf(vertex)) {
			cutBranch(v);
		}
		// and delete yourself and edge to parent
		cutHelper(vertex);
	}

	/**
	 * <p>
	 * Removes a vertex from the tree. If the given vertex doesn't exist there are no effects. Note that it does not
	 * remove any direct or indirect children. Also, this will leave this <code>Tree</code> object in a state in which
	 * it does <i>not</i> represent a tree anymore!
	 * </p>
	 * Note: it will not remove a descriptor, even though it doesn't have any vertices left attached to it.
	 * 
	 * @param vertex
	 *            A tree vertex to remove.
	 */
	public void cutVertex(TreeVertex vertex) {
		if (!containsVertex(vertex)) {
			return;
		}

		// Separate set for iteration in order to avoid concurrency problems...
		Set<DefaultEdge> delE = new HashSet<DefaultEdge>(outgoingEdgesOf(vertex));
		for (DefaultEdge e : delE) {
			removeEdge(e);
		}
		cutHelper(vertex);
	}

	/**
	 * A helper method for cutBranch and cutVertex so that the code is reused. It deletes vertex itself and also deletes
	 * the edges to its parent vertex.
	 */
	private void cutHelper(TreeVertex vertex) {
		Descriptor desc = getDescriptorByVertex(vertex);
		desc.removeTreeVertex(vertex);
		removeEdge(incomingEdgeOf(vertex));
		removeVertex(vertex);
		vertexName2vertex.remove(vertex.getName());
	}

	/**
	 * <p>
	 * Removes a descriptor and all of its vertices.
	 * </p>
	 * Note that selecting <code>mode</code> = 1 this will potentially leave this object in a invalid state as it may
	 * cut tree vertices which are not leaves, thus cutting this <code>Tree</code> into partitions.
	 * 
	 * @param desc
	 *            A descriptor
	 * @param mode
	 *            Selects the mode for cutting a descriptor. If set to <code>CUT_ONLY</code> it will simply cut all tree
	 *            vertices of <code>desc</code> leaving the tree potentially partitioned. If set to
	 *            <code>CUT_AND_SLIDEUP</code> it will cut all tree vertices of <code>desc</code> but then slide now
	 *            disconnected tree vertices up towards the root vertex in order to keep the tree valid (-> connected).
	 */
	public void cutDescriptor(Descriptor desc, int mode) {
		// just iterating over desc.getTreeVertices() gives ConcurrentModificationException...
		// so collect vertices to delete first, then do deletion itself
		Set<TreeVertex> delV = new LinkedHashSet<TreeVertex>(desc.getTreeVertices());

		if (mode == CUT_ONLY) {
			for (TreeVertex v : delV) {
				cutVertex(v);
			}
		} else if (mode == CUT_AND_SLIDEUP) {
			TreeFilter filter = new TreeFilter(this, TreeFilter.KEEP, TreeFilter.THROW);
			filter.maskDesc(desc, TreeFilter.THROW, false);
			filter.apply();
		} else {
			logger.warn("�nvalid mode selected for cutDescriptor: mode " + mode);
			return;
		}

		descName2desc.remove(desc.getName());
		descUi2desc.remove(desc.getUI());
	}

	/**
	 * Renames a vertex <code>vertex</code> to <code>newName</code>. It cannot be renamed if there is another
	 * tree-vertex with name <code>newName</code>.
	 * 
	 * @param vertex
	 *            A tree vertex.
	 * @param newName
	 *            The new name for <code>vertex</code>.
	 * @return Returns true on successful renaming, false else.
	 */
	public boolean renameVertex(TreeVertex vertex, String newName) {
		if (!containsVertex(vertex)) {
			logger.warn("Could not rename vertex - the vertex to rename doesn't exist in my data: " + vertex);
			return false;
		}
		if (vertex.getName().equals(newName)) {
			// nothing to do.
			return true;
		}
		if (vertexName2vertex.containsKey(newName)) {
			logger.warn("Could not rename vertex - new name is already in use. Vertex was " + vertex
					+ "  New Name/Name in use was " + newName);
			return false;
		}
		vertexName2vertex.remove(vertex.getName());
		vertex.setName(newName);
		vertexName2vertex.put(newName, vertex);
		return true;

	}

	/**
	 * Moves a branch to another location in the tree. In fact, it just assigns a given root of a branch a new parent
	 * node. However, please note that it will change the partial tree number of a tree-node if it otherwise wouldn't be
	 * unique within its siblings anymore.
	 * 
	 * @param vertexName
	 *            Name of the root-vertex of the branch to move.
	 * @param newParentVertexName
	 *            Name of the new parent of vertex, i.e. the target were to move it to.
	 * @return Returns true if branch was successfully moves; false otherwise.
	 */
	public boolean moveBranch(String vertexName, String newParentVertexName) {
		TreeVertex v = getVertex(vertexName);
		TreeVertex p = getVertex(newParentVertexName);
		if (v == null) {
			logger.warn("Could not move branch, because 'vertex to move' doesn't exist in my data. vertex = "
					+ vertexName + " :: new parent = " + newParentVertexName);
			return false;
		} else if (p == null) {
			logger.warn("Could not move branch, because 'new parent vertex' doesn't exist in my data. vertex = "
					+ vertexName + " :: new parent = " + newParentVertexName);
			return false;
		}
		return moveBranch(v, p);
	}

	/**
	 * Moves a branch to another location in the tree. In fact, it just assigns a given root of a branch a new parent
	 * node. However, please note that it will change the partial tree number of a tree-node if it otherwise wouldn't be
	 * unique within its siblings anymore.
	 * 
	 * @param vertex
	 *            The root of the branch to move.
	 * @param newParent
	 *            The new parent of vertex, i.e. the target were to move it to.
	 * @return Returns true if branch was successfully moves; false otherwise.
	 */
	public boolean moveBranch(TreeVertex vertex, TreeVertex newParent) {

		if (newParent == null || vertex == null || !containsVertex(newParent) || !containsVertex(vertex)) {
			if (newParent == null) {
				logger.warn("Could not move branch, because target vertex == null. Branch to move roots in " + vertex);
			} else if (vertex == null) {
				logger.warn("Could not move branch, because 'root of branch to move' == null");
			} else if (!containsVertex(newParent)) {
				logger.warn("Could not move branch, because target vertex " + newParent + " is not in data");
			} else { // if (!containsVertex(vertex)) {
				logger.warn("Could not move branch, because 'root of branch to move' " + vertex + " is not in data.");
			}
			return false;
		}

		// prevent invalid moving: <code>newVertex</code> is an offspring of <code>vertex</code>
		if (isAnchestorVertex(vertex.getName(), newParent)) {
			logger.warn("invalid moving of '" + vertex.toString() + "' to new parent '" + newParent.toString()
					+ "'. New parent is offspring of vertex to move.");
			return false;
		}

		// nothing to move ...
		if (vertex.equals(newParent)) {
			return true;
		}

		// get all partial tree-numbers of children of new parent ...
		Set<String> partTreeNr = new LinkedHashSet<String>();
		for (TreeVertex v : childVerticesOf(newParent)) {
			partTreeNr.add(v.getPartialTreeNumber());
		}
		// ... and calculate partial tree number for its new child (if
		// necessary)
		String newNr = vertex.getPartialTreeNumber();
		if (partTreeNr.contains(vertex.getPartialTreeNumber())) {
			int nr = 1;
			while (partTreeNr.contains(Integer.toString(nr))) {
				nr++;
			}
			newNr = Integer.toString(nr);
		}
		vertex.setPartialTreeNumber(newNr);

		// remove current edge, then add new edge
		removeEdge(incomingEdgeOf(vertex));
		addEdge(newParent, vertex);

		// reset height of everything in the moved branch
		invalidateHeights(vertex);

		return true;
	}

	/**
	 * To determine the parent vertex of a given vertex. Note that in a tree, each vertex at most got one parent.
	 * 
	 * @param v
	 *            A child of the vertex to find.
	 * @return Returns the parent vertex of v, i.e. a vertex p so that this.containsEdge(p, v) == true holds. If there
	 *         is no such vertex (i.e. v is the root of the tree, or the parent is pending) then null is returned.
	 */
	public TreeVertex parentVertexOf(TreeVertex v) {
		DefaultEdge edge = incomingEdgeOf(v);
		if (edge == null) {
			return null;
		} else {
			return this.getEdgeSource(edge);
		}
	}

	/**
	 * To determine the parent vertex of a given vertex. Note that in a tree, each vertex at most got one parent.
	 * 
	 * @param vName
	 *            Name of a child of the vertex to find.
	 * @return Returns the parent vertex of v, i.e. a vertex p so that this.containsEdge(p, v) == true holds. If there
	 *         is no such vertex (i.e. v is the root of the tree, or the parent is pending, or no vertex with such a
	 *         name) then null is returned.
	 */
	public TreeVertex parentVertexOf(String vName) {
		TreeVertex v = getVertex(vName);
		if (v == null) {
			return null;
		} else {
			return parentVertexOf(v);
		}
	}

	/**
	 * @param parent
	 *            Potential parent tree vertex of v.
	 * @param v
	 *            A tree vertex.
	 * @return Returns true if parent is the parent vertex of v. False else. Also false if any of parent, v or
	 *         parentVertexOf(v) is null.
	 */
	public boolean isParentVertex(TreeVertex parent, TreeVertex v) {
		if (parent == null | v == null) {
			return false;
		}
		return parent.equals(parentVertexOf(v));
	}

	/**
	 * 
	 * @param anchestorName
	 *            Name of a tree vertex.
	 * @param v
	 *            A tree vertex.
	 * @return Returns <code>true</code> iff <code>anchestor</code> is an anchestor vertex of <code>v</code>. Otherwise
	 *         is returns <code>false</code>. It does not return <code>true</code> if <code>anchestor</code> and
	 *         <code>v</code> are the same vertex.
	 */
	public boolean isAnchestorVertex(String anchestorName, TreeVertex v) {
		if (v.getName().equals(root.getName())) {
			return false;
		}
		return isAnchestorVertex_internal(anchestorName, v);
	}

	private boolean isAnchestorVertex_internal(String anchestorName, TreeVertex v) {
		TreeVertex parent = parentVertexOf(v);
		String pName = parent.getName();

		if (pName.equals(anchestorName)) {
			return true;
		}

		if (pName.equals(root.getName())) {
			return false;
		}

		return isAnchestorVertex(anchestorName, parent);
	}

	/**
	 * Note: as this class is representing a tree, a single vertex should never have more than one parent. Is this the
	 * case a error message will be written, and null is returned.
	 * 
	 * @return Returns an incoming edge of v. More in detail it returns the first element when iterating through
	 *         this.incomingEdges(). If there is not such element it returns null.
	 */
	private DefaultEdge incomingEdgeOf(TreeVertex v) {
		if (v == null || !containsVertex(v) || inDegreeOf(v) == 0) {
			if (v == null) {
				logger.warn("Could not get parent vertex, because argument was 'null'");
			} else if (!containsVertex(v)) {
				logger.warn("Could not get parent vertex, because vertex is not my data. vertex was: " + v);
			} else {
				if (!v.equals(root))
					logger.warn("Could not get parent vertex, because it doesn't have a parent. Vertex was: " + v);
			}
			return null;
		}
		Set<DefaultEdge> edgeSet = incomingEdgesOf(v);
		if (edgeSet.isEmpty()) {
			// then v is a root element
			return null;
		} else {
			// there should be only one parent, otherwise it's not a tree
			// according to our expectations
			if (edgeSet.size() == 1) {
				return edgeSet.iterator().next();
			} else {
				logger.error("A vertex got more than one incoming edges! Vertx was : " + v);
				return null;
			}
		}
	}

	/**
	 * @return Returns the set of children of v and the empty set if v has no children.
	 */
	public List<TreeVertex> childVerticesOf(TreeVertex v) {
		List<TreeVertex> children = new ArrayList<TreeVertex>();
		if (v == null) {
			logger.warn("Could not get child vertices of v because v == null");
		} else if (!containsVertex(v)) {
			logger.warn("Could not get child vertices of v because v is not in my data. v was : " + v);
		} else {
			for (DefaultEdge e : outgoingEdgesOf(v)) {
				children.add(getEdgeTarget(e));
			}
		}
		Collections.sort(children);
		// for (TreeVertex t : children)
		// System.out.println("CHILD: " + t.toFullString(this));
		return children;
	}

	/**
	 * 
	 * @param d
	 *            The descriptor for which to get its children.
	 * @return A list of child descriptors according to their tree vertices.
	 */
	public List<Descriptor> childDescriptorsOf(Descriptor d) {
		List<Descriptor> ret = new ArrayList<>();
		for (TreeVertex treeVertex : d.getTreeVertices()) {
			for (DefaultEdge e : outgoingEdgesOf(treeVertex)) {
				TreeVertex childVertex = getEdgeTarget(e);
				Descriptor childDescriptor = getDescriptorByVertex(childVertex);
				ret.add(childDescriptor);
			}
		}
		return ret;
	}

	/**
	 * @param desc
	 *            A descriptor.
	 * @param nr
	 *            A tree number.
	 * @return Returns true if descriptor <code>desc</code> has a tree number <code>nr</code>; false otherwise.
	 */
	public boolean hasTreeNumber(Descriptor desc, TreeNumber nr) {
		return allTreeNumbersOf(desc).contains(nr);
	}

	/**
	 * @return Returns the full tree number of a vertex in this tree
	 */
	public TreeNumber treeNumberOf(TreeVertex v) {
		// get tree number as the concatenated tree numbers of parent and own
		// partial tree number
		TreeVertex parent = parentVertexOf(v);
		if (parent == null || parent.equals(root)) {
			return new TreeNumber(v.getPartialTreeNumber());
		} else {
			return new TreeNumber(treeNumberOf(parent) + "." + v.getPartialTreeNumber());
		}
	}

	/**
	 * @param d
	 *            The descriptor of which all tree-numbers to get.
	 * @return Returns a set containing all tree-number which refer to the descriptor d.
	 */
	public Set<TreeNumber> allTreeNumbersOf(Descriptor d) {
		Set<TreeNumber> treeNumbers = new LinkedHashSet<TreeNumber>();
		for (TreeVertex v : d.getTreeVertices()) {
			treeNumbers.add(this.treeNumberOf(v));
		}
		return treeNumbers;
	}

	/**
	 * Retrieves a TreeVertex by its name.
	 * 
	 * <p>
	 * Note: Use this method with care when you use a tree-number for <code>vertexName</code> since real tree numbers
	 * can be subject to dynamic changes. However, the name of a vertex will not change just because the position of it
	 * changed!
	 * </p>
	 * 
	 * @param orgTreeNr
	 *            A original tree number, i.e. the number of the tree node as it was used in the original MeSH XML.
	 * @return Returns the corresponding tree vertex to the original tree number or null if there is no such tree
	 *         vertex.
	 */
	public TreeVertex getVertex(String vertexName) {
		return vertexName2vertex.get(vertexName);
	}

	/**
	 * Checks if a certain vertex is contained in this tree.
	 * 
	 * @param vertexName
	 *            Name of a tree vertex.
	 * @return returns true of there is a vertex with name <code>vertexName</code>; false otherwise.
	 */
	public boolean hasVertex(String vertexName) {
		return vertexName2vertex.containsKey(vertexName);
	}

	/**
	 * <p>
	 * This adds a descriptor and its tree vertices to a tree instance. This is the method of choice to add new data to
	 * the tree.
	 * </p>
	 * <p>
	 * This method will not add <code>desc</code> but return false if one or more of the following conditions hold:
	 * <ul>
	 * <li>a descriptor with equal name exists</li>
	 * <li>a descriptor with equal ui exists</li>
	 * <li><code>desc</code> contains a tree-vertex with a name that is equal to another tree-vertex in this tree.</li>
	 * </ul>
	 * If the descriptor was added successfully it will return true.
	 * </p>
	 * <p>
	 * It will add appropriate vertices and edges to the graph for the provided descriptor <code>desc</code> and its
	 * tree-vertices. It will also add the descriptor to the internal HashMap. While <code>vertexNames</code> gives the
	 * names of all vertices of <code>desc</code>, <code>parentVertexNames</code> states where to add each vertex, i.e.
	 * as a direct child of which tree-vertex a vertex will be added.
	 * </p>
	 * <p>
	 * Descriptors don't need to be added in any particular order. (Actually, this is often not possible, since one
	 * descriptor often refers to more than one tree-number.)
	 * </p>
	 * <p>
	 * vertexNames and parentVertexNames are strings and can be anything. However, please note that "."s are treated in
	 * a special way to determine the tree-number of a vertex. So if you don't use tree-numbers as vertexNames, you
	 * should maybe not use "." in it, since only the part after the last "." in the name will be used as the
	 * tree-vertex's name when determining dynamic tree-numbers.
	 * </p>
	 * 
	 * @param desc
	 *            a descriptor.
	 * @param parentVertexNames
	 *            Array of names of the parent vertices.
	 * @param vertexNames
	 *            Names of the vertices to add. The entry of <code>parentVertexNames</code> with the same index refers
	 *            to its parent.
	 * @return Returns true if descriptor was added successfully; false otherwise.
	 */
	public boolean addDescriptor(Descriptor desc, String[] parentVertexNames, String[] vertexNames) {

		// check if we can add desc
		if (descName2desc.containsKey(desc.getName())) {
			String locs = "";
			for (String loc : vertexNames) {
				locs = locs + loc + " : ";
			}
			logger.warn("Could not add descriptor because its name is already in use. Descriptor to add was:\n"
					+ desc.tofullString(this) + "@ " + locs + "It is conflicting with:\n"
					+ descName2desc.get(desc.getName()).tofullString(this));
			return false;
		}
		if (descUi2desc.containsKey(desc.getUI())) {
			logger.warn("Could not add descriptor because its UI is already in use. Descriptor was:  " + desc);
			return false;
		}
		for (TreeVertex v : desc.getTreeVertices()) {
			if (vertexName2vertex.containsKey(v.getName())) {
				logger.warn("Could not add descriptor because one of its tree vertices has a name that is already in use. Descriptor was:  "
						+ desc + " -- Vertex was: " + v);
				return false;
			}
		}

		// for each tree-number of that descriptor create a vertex and edge, and add it to the graph
		for (int i = 0; i < parentVertexNames.length; i++) {
			addTreeVertexToDesc(desc, parentVertexNames[i], vertexNames[i]);
		}

		descName2desc.put(desc.getName(), desc);
		descUi2desc.put(desc.getUI(), desc);

		return true;
	}

	/**
	 * This adds a new descriptor with a single tree-vertex. The method is a Wrapper for
	 * <code>addDescriptor (Descriptor, String[], String[])</code>. See there for more details.
	 */
	public boolean addDescriptor(Descriptor desc, String parentVertexName, String vertexName) {
		String[] parentVertexNames = { parentVertexName };
		String[] vertexNames = { vertexName };
		return addDescriptor(desc, parentVertexNames, vertexNames);
	}

	/**
	 * Wrapper for <code>addDescriptor (Descriptor, String[], String[])</code>. For this call a <code>TreeNumber</code>
	 * is expected, which serves as the name of the vertex and also carries implicitly the structure information (i.e.
	 * where the vertex should be inserted). Make sure you understand what you are doing when you using this method.
	 */
	public boolean addDescriptor(Descriptor desc, TreeNumber treeNr) {
		List<TreeNumber> tmpSet = new ArrayList<TreeNumber>(1);
		tmpSet.add(treeNr);
		return addDescriptor(desc, tmpSet);
	}

	/**
	 * Wrapper for <code>addDescriptor (Descriptor, String[], String[])</code>. For this call <code>TreeNumber</code>s
	 * are expected, which serve as names for the vertices and also carry implicitly the structure information (i.e.
	 * where a vertex should be inserted). Make sure you understand the role of <code>treeNrs</code> when you use this
	 * method.
	 */
	public boolean addDescriptor(Descriptor desc, List<TreeNumber> treeNrs) {
		String[] parentVertexNames = new String[treeNrs.size()];
		String[] vertexNames = new String[treeNrs.size()];

		// get parent vertex names and own names as strings
		int i = 0;
		for (TreeNumber treeNr : treeNrs) {
			TreeNumber tmp = treeNr.getParentNumber();
			if (tmp == null) {
				// Check whether we have the "facets" of the top terms, i.e. for the MeSH the MeSH categories like
				// "Anatomy", "Diseases", "Chemicals and Drugs", ...
				String firstChar = treeNr.getNumber().substring(0, 1);
				if (getVertex(firstChar) != null)
					parentVertexNames[i] = firstChar;
				else
					parentVertexNames[i] = getRootVertex().getName();
			} else {
				parentVertexNames[i] = tmp.toString();
			}
			vertexNames[i] = treeNr.toString();
			i++;
		}

		// add descriptor
		return addDescriptor(desc, parentVertexNames, vertexNames);
	}

	/**
	 * Wrapper for <code>addDescriptor (Descriptor, String[], String[])</code>. If a child doesn't have a parent, the
	 * value for parent should be <code>null</code> or the empty string.
	 */
	public boolean addDescriptor(Descriptor desc, VertexLocations childNames2parentNames) {
		String[] parentVertexNames = new String[childNames2parentNames.size()];
		String[] vertexNames = new String[childNames2parentNames.size()];
		// get parent vertex names and own names as strings
		int i = 0;
		for (String child : childNames2parentNames.getVertexNameSet()) {
			String parent = childNames2parentNames.get(child);
			if (parent == null || parent.equals("")) {
				parentVertexNames[i] = getRootVertex().getName();
			} else {
				parentVertexNames[i] = parent;
			}
			vertexNames[i] = child;
			i++;
		}
		// add descriptor
		return addDescriptor(desc, parentVertexNames, vertexNames);
	}

	/**
	 * <p>
	 * Adds a new vertex with name <code>vertexName</code> of a descriptor <code>desc</code> to this <code>Tree</code>
	 * instance (and also to the descriptor itself). It is added as a child of the vertex with name
	 * <code>parentVertexName</code>. This is different to Descriptor.addTreeVertex(TreeVertex) as the latter only adds
	 * it to the descriptor, but not to the representation of the MeSH (i.e. the Tree instance).
	 * <p>
	 * <p>
	 * If there is already a vertex with the same name, no new vertex will be added and null will be returned.
	 * </p>
	 * <p>
	 * Internal note: in fact it only tries to add the given TreeVertex to descriptor, as it might not be possible yet,
	 * since the parent for the tree-vertex might still be missing. In that case it is added to a waiting list and as
	 * soon as the missing parent is added the tree-vertex will be added too.
	 * </p>
	 * 
	 * @param desc
	 *            A descriptor.
	 * @param parentVertexName
	 *            Name of the parent vertex.
	 * @param vertexName
	 *            Name of the vertex of <code>desc</code> to add.
	 * @return Returns the added tree vertex, or null if the tree vertex could not be added.
	 */
	public TreeVertex addTreeVertexToDesc(Descriptor desc, String parentVertexName, String vertexName) {
		if (vertexName2vertex.containsKey(vertexName)) {
			logger.warn("Could not add tree vertex its name is already in use. vertex-name was: " + vertexName);
			return null;
		}

		// build and add vertex - that's always possible
		TreeVertex vertex = new TreeVertex(new TreeNumber(vertexName).getLastPartialNumber(), vertexName,
				desc.getName(), desc.getUI());
		addVertex(vertex);
		desc.addTreeVertex(vertex);

		// check if parent vertex is already added ...
		if (vertexName2vertex.containsKey(parentVertexName)) {
			// if it is, just add the edge to parent
			TreeVertex parent = vertexName2vertex.get(parentVertexName);
			addEdge(parent, vertex);
		} else {
			// if not, then remember it as a pending tree-number
			addPendingVertex(parentVertexName, vertex);
		}

		// also try to add edges to children of current tree number (since
		// edges for them couldn't be added yet)
		tryPendingVertices(vertex);

		// also, add own vertex name to hash table so that children or
		// parents can know of it
		vertexName2vertex.put(vertexName, vertex);

		return vertex;
	}

	public TreeVertex addTreeVertexToDesc(String descUi, String parentVertexName, String vertexName) {
		if (!descUi2desc.containsKey(descUi)) {
			logger.warn("Could not add '" + vertexName + "' as child of '" + parentVertexName
					+ "' because the descriptor UI '" + descUi + "' doesn't exist.");
			return null;
		} else {
			return addTreeVertexToDesc(descUi2desc.get(descUi), parentVertexName, vertexName);
		}
	}

	/**
	 * Changes the UI of descriptor <code>desc</code> to <code>newUi</code>.
	 * 
	 * @param desc
	 *            The descriptor of which the UI is to be changed.
	 * @param newUi
	 *            The new UI for desc.
	 * @result True if UI was changed, false if not.
	 */
	public boolean changeUiOf(Descriptor desc, String newUi) {

		if (desc == null) {
			logger.warn("Could not change UI of desc because desc == null");
			return false;
		} else if (!hasDescriptorByUi(desc.getUI())) {
			logger.warn("Could not change UI of desc because it is not my data. desc was " + desc);
			return false;
		} else if (hasDescriptorByUi(newUi)) {
			logger.warn("Could not change UI of desc because new UI is already in use. desc was " + desc);
			return false;
		}

		descUi2desc.remove(desc.getUI());
		for (TreeVertex v : desc.getTreeVertices()) {
			v.setDescUi(newUi);
		}
		desc.setUI(newUi);
		descUi2desc.put(newUi, desc);
		return true;
	}

	/**
	 * Changes the name of descriptor <code>desc</code> to <code>newName</code>.
	 * 
	 * @param desc
	 *            The descriptor of which the name is to be changed.
	 * @param newName
	 *            The new name for desc.
	 * @result True if name was changed, false if not.
	 */
	public boolean changeNameOf(Descriptor desc, String newName) {

		if (desc == null) {
			logger.warn("Could not change name of desc because desc == null");
			return false;
		} else if (!hasDescriptorByUi(desc.getUI())) {
			logger.warn("Could not change name of desc because it is not my data. desc was " + desc);
			return false;
		} else if (hasDescriptorByName(newName)) {
			logger.warn("Could not change name of desc because new name is already in use. desc was " + desc);
			return false;
		}

		descName2desc.remove(desc.getName());
		for (TreeVertex v : desc.getTreeVertices()) {
			v.setDescName(newName);
		}
		desc.setName(newName);
		descName2desc.put(newName, desc);
		return true;
	}

	/**
	 * Changes the descriptor a tree-vertex is bound to.
	 * 
	 * @param v
	 *            A tree vertex.
	 * @param desc
	 *            A descriptor.
	 * @return true on success, false otherwise.
	 */
	public boolean changeDescOf(TreeVertex v, Descriptor desc) {
		if (v == null) {
			logger.warn("Could not change desc of vertex v because v == null");
			return false;
		} else if (desc == null) {
			logger.warn("Could not change desc of vertex because desc == null. vertex was " + v);
			return false;
		} else if (!hasVertex(v.getName())) {
			logger.warn("Could not change desc of vertex because vertex is not in my data. vertex was " + v);
			return false;
		} else if (!hasDescriptorByUi(desc.getUI())) {
			logger.warn("Could not change desc of vertex because desc is not my data. vertex was " + v);
			return false;
		}

		Descriptor curDesc = this.getDescriptorByVertex(v);
		curDesc.removeTreeVertex(v);
		desc.addTreeVertex(v);

		v.setDescUi(desc.getUI());
		v.setDescName(desc.getName());

		return true;
	}

	/**
	 * @param descName
	 *            Name of a descriptor.
	 * @return Returns true of a descriptor with name <code>descName</code> exists.
	 */
	public boolean hasDescriptorByName(String descName) {
		return descName2desc.containsKey(descName);
	}

	/**
	 * @param descName
	 *            A descriptors name.
	 * @return Returns the corresponding descriptor with name descName, or null if no such descriptor exists.
	 */
	public Descriptor getDescriptorByName(String descName) {
		return descName2desc.get(descName);
	}

	/**
	 * @param descUi
	 *            Name of a descriptor.
	 * @return Returns true of a descriptor with UI <code>descUi</code> exists.
	 */
	public boolean hasDescriptorByUi(String descUi) {
		return descUi2desc.containsKey(descUi);
	}

	/**
	 * @param descUi
	 *            A descriptors UI (http://www.nlm.nih.gov/mesh/xml_data_elements .html#DescriptorUI)
	 * @return Returns the corresponding descriptor with UI descUi, or null if no such descriptor exists.
	 */
	public Descriptor getDescriptorByUi(String descUi) {
		return descUi2desc.get(descUi);
	}

	/**
	 * @param v
	 *            A tree vertex.
	 * @return Returns the descriptor that belongs to tree vertex v or null if there is no such descriptor.
	 */
	public Descriptor getDescriptorByVertex(TreeVertex v) {
		return getDescriptorByUi(v.getDescUi());
	}

	/**
	 * @param v
	 *            A tree vertex.
	 * @return Returns the parents descriptor of tree vertex v.
	 */
	public Descriptor parentDescriptorOf(TreeVertex v) {
		TreeVertex parent = parentVertexOf(v);
		if (parent != null)
			return getDescriptorByVertex(parent);
		return null;
	}

	/**
	 * 
	 * @param d
	 *            Descriptor for which to get its parent descriptors.
	 * @return A list of descriptors that are parents of <tt>d</tt> via their associated tree vertices.
	 */
	public List<Descriptor> parentDescriptorsOf(Descriptor d) {
		List<Descriptor> ret = new ArrayList<>();
		for (TreeVertex treeVertex : d.getTreeVertices()) {
			TreeVertex parentVertex = parentVertexOf(treeVertex);
			Descriptor parentDesc = getDescriptorByVertex(parentVertex);
			ret.add(parentDesc);
		}
		return ret;
	}

	/**
	 * @param desc
	 *            A descriptor.
	 * @return Returns a list containing the descriptorUIs of all parents of desc.
	 *         (http://www.nlm.nih.gov/mesh/xml_data_elements.html# DescriptorUI).
	 */
	public List<String> allParentUIsOf(Descriptor desc) {
		List<String> allUis = new LinkedList<String>();
		for (TreeVertex v : desc.getTreeVertices()) {
			allUis.add(parentDescriptorOf(v).getUI());
		}
		return allUis;
	}

	/**
	 * @param desc
	 *            A descriptor
	 * @return Returns the "best" tree vertex of desc, which is the one with the smallest height.
	 */
	public TreeVertex getBestTreeVertexOf(Descriptor desc) {
		TreeVertex best = null;
		for (TreeVertex v : desc.getTreeVertices()) {
			if (best == null) {
				best = v;
			} else if (heightOf(best) > heightOf(v)) {
				best = v;
			}
		}
		return best;
	}

	/**
	 * Returns the height of a vertex, relative to root. There must be a path from root to v so that the height can be
	 * determined. If there is a loop, this method will never return. If there is no path from root to vertex it will
	 * give a NullPointerException.
	 * 
	 * @param v
	 *            A tree vertex.
	 * @return Returns the heights of vertex v. The heights of the root-vertex is 0.
	 */
	public int heightOf(TreeVertex v) {
		if (!v.hasValidHeight()) {
			updateHeight(v);
		}
		return v.getHeight();
	}

	/**
	 * Seems like this method is not needed any longer since heights is cached now and calculated in a similar but
	 * easier way... keep it for now tough.
	 * 
	 * Determines the height of a vertex, relative to root. There must be a path from root to v so that the height can
	 * be determined. If there is a loop, this method will never return. If there is no path from root to vertex it will
	 * give a NullPointerException.
	 * 
	 * @param v
	 *            A tree vertex.
	 * @return Returns the heights of vertex v. The heights of the root-vertex is 0.
	 */
	@SuppressWarnings("unused")
	private int calcHeightOf(TreeVertex v) {
		if (v.equals(root)) {
			return 0;
		} else {
			return heightOf(this.parentVertexOf(v)) + 1;
		}
	}

	private void updateHeight(TreeVertex v) {
		TreeVertex parent = parentVertexOf(v);
		if (parent != null) {
			v.setHeight(heightOf(parent) + 1);
		}
	}

	/**
	 * Determines the height of a descriptor, relative to root. The height of a descriptor is the minimum height of any
	 * tree-vertex of that descriptor.
	 * 
	 * @param desc
	 *            A descriptor
	 * @return Returns the heights desc, or -1 if it has no height.
	 */
	public int heightOf(Descriptor desc) {
		int min = Integer.MAX_VALUE;
		for (TreeVertex v : desc.getTreeVertices()) {
			int h = heightOf(v);
			if (h < min) {
				min = h;
			}
		}
		// if no tree vertex had a height
		if (min == Integer.MAX_VALUE) {
			return -1;
		}
		return min;
	}

	/**
	 * Sets height of v and it's children to invalid. This is necessary after v is moved.
	 * 
	 * @param v
	 *            A tree vertex.
	 */
	private void invalidateHeights(TreeVertex v) {
		for (TreeVertex c : childVerticesOf(v)) {
			invalidateHeights(c);
		}
		v.setHeightInvalid();
	}

	/**
	 * Marks a tree vertex with name <code>parentVertexName</code> as pending, meaning that vertex <code>v</code> is
	 * waiting that it gets added, so that its edge to it can be added in the graph as well. This is not yet possible
	 * until the parent itself is added to the graph.
	 * 
	 * @param parentVertexName
	 *            Name of parent vertex <code>v</code> is waiting for.
	 * @param v
	 *            A vertex.
	 */
	private void addPendingVertex(String parentVertexName, TreeVertex v) {
		List<TreeVertex> list;
		if (pendingVertices.containsKey(parentVertexName)) {
			list = pendingVertices.get(parentVertexName);
		} else {
			list = new LinkedList<TreeVertex>();
		}
		list.add(v);
		pendingVertices.put(parentVertexName, list);
	}

	/**
	 * Adds all edges from v to children of v that have been parsed prior to v and are thus 'pending'.
	 * 
	 * @param v
	 *            a vertex.
	 */
	private void tryPendingVertices(TreeVertex v) {
		String vertexName = v.getName();
		if (!pendingVertices.containsKey(vertexName)) {
			return;
		} else {
			for (TreeVertex c : pendingVertices.get(vertexName)) {
				addEdge(v, c);
			}
			// they are not pending anymore...
			pendingVertices.remove(vertexName);
		}
	}

	/**
	 * Print out some info about the object state
	 * 
	 * @param out
	 */
	public void printInfo(PrintStream out) {
		int waitCnt = 0;

		int nrOfTerms = 0;
		for (Descriptor desc : descName2desc.values()) {
			for (Concept c : desc.getConcepts()) {
				nrOfTerms += c.size();
			}
		}

		out.println();
		out.println("# Some information about the tree '" + getName() + "': ");
		out.println(" Number of pending vertices : " + pendingVertices.size());
		// -> they are still missing in the data, but we need them to properly add edges to their children which are
		// already added
		int i = 0;
		for (String name : pendingVertices.keySet()) {

			waitCnt += pendingVertices.get(name).size();

			out.println("   pending for vertex '" + name + "' are these vertices  ::  ");
			for (TreeVertex v : pendingVertices.get(name)) {
				Descriptor desc = getDescriptorByVertex(v);
				out.println("      " + v.getName() + " of descriptor '" + desc.getName() + " : " + desc.getUI() + "'");
			}
			i++;
			if (i > 50) {
				out.println("There are " + (pendingVertices.size() - 50) + " more pending vertices! ... I skip them.");
				break;
			}
		}
		out.println(" Number of pending vertices : " + pendingVertices.size());
		out.println(" Number of waiting vertices : " + waitCnt);
		out.println(" Number of descriptors : " + descName2desc.size());
		out.println(" Number of terms: " + nrOfTerms);
		out.println(" Number of vertices : " + vertexSet().size());
		out.println(" Number of edges : " + edgeSet().size());
	}

	/**
	 * Checks that the tree is valid, which requires:
	 * <ol>
	 * 
	 * <li>Tree Vertices</li>
	 * <ul>
	 * <li>per vertex</li>
	 * <ul>
	 * <li>must belong to a descriptor</li>
	 * <li>must have exactly one parent. root must have no parent</li>
	 * <li>link to descName and descUi must be valid and link to the same descriptor</li>
	 * <li>must have a height >= 0</li>
	 * </ul>
	 * <li>mapping</li>
	 * <ul>
	 * <li>vertexNam2vertex mapping must be correct and valid</li>
	 * </ul>
	 * </ul>
	 * 
	 * <li>Descriptors</li>
	 * <ul>
	 * <li>per descriptor</li>
	 * <ul>
	 * <li>all its tree vertices must link back to it</li>
	 * <li>all its tree vertices must be contained in the tree</li>
	 * <li>name and ui of it must map back to it in descName2desc and descUi2desc</li>
	 * </ul>
	 * <li>mapping</li>
	 * <ul>
	 * <li>descName2desc mappings must be correct and valid</li>
	 * <li>descUi2desc mappings must be correct and valid</li>
	 * </ul>
	 * </ul>
	 * 
	 * <li>others</li>
	 * <ul>
	 * <li>there are no pending or waiting vertices</li>
	 * </ul>
	 * 
	 * </ol>
	 * 
	 * @return True if ok, false if not.
	 */
	public boolean verifyIntegrity() {
		boolean validFlag = true;
		String msg = "Verification of of " + this.getName() + " failed : ";

		try {

			// pending / waiting vertices
			if (pendingVertices.size() != 0) {
				logger.error(msg + "There are " + pendingVertices.size() + " pending vertices.");
				validFlag = false;
			}

			// tree-vertices
			for (TreeVertex v : this.vertexSet()) {
				if (!this.hasDescriptorByName(v.getDescName())) {
					logger.error(msg + v + " is linked to descriptor name " + v.getDescName()
							+ " which is not in 'descName2desc2'.");
					validFlag = false;
				}
				if (!this.hasDescriptorByUi(v.getDescUi())) {
					logger.error(msg + v + " is linked to descriptor UI " + v.getDescUi()
							+ " which is not in 'descUi2desc2'.");
					validFlag = false;
				}
				if (!getDescriptorByUi(v.getDescUi()).equals(getDescriptorByName(v.getDescName()))) {
					logger.error(msg + "descName and descUi link to different descriptors in tree vertex " + v + " : ");
					logger.error(msg + "descUi-link: " + getDescriptorByUi(v.getDescUi()));
					logger.error(msg + "descName-link: " + getDescriptorByName(v.getDescName()));
					validFlag = false;
				}
				if (!v.equals(root)) {
					if (null == this.parentVertexOf(v)) {
						logger.error(msg + "tree vertex doesn't have a parent: " + v);
						validFlag = false;
					} else if (this.inDegreeOf(v) != 1) {
						logger.error(msg + "vertex " + v + " must have 1 parent, but it got " + this.inDegreeOf(v));
						validFlag = false;
					}
				}
				if (this.heightOf(v) < 0) {
					logger.error(msg + "height = " + this.heightOf(v) + " is not valid, in tree vertex " + v);
					validFlag = false;
				}
			}

			// tree-vertex mappings
			for (String name : vertexName2vertex.keySet()) {
				TreeVertex v = vertexName2vertex.get(name);
				if (!this.containsVertex(v)) {
					logger.error(msg + "mapping in 'vertexName2vertex' is not contained in the tree: " + v);
					validFlag = false;
				} else if (!v.getName().equals(name)) {
					logger.error(msg + "invalid name-vertex mapping in 'verteName2vertex : 'key = " + name
							+ " : value = " + v.getName() + "'.");
					validFlag = false;
				}
			}

			// descriptors
			for (Descriptor d : descName2desc.values()) {
				for (TreeVertex v : d.getTreeVertices()) {
					if (!this.containsVertex(v)) {
						logger.error(msg + "Descriptor " + d + " has a tree vertex " + v
								+ " which is not contained in the tree.");
						validFlag = false;
					} else {
						if (!v.getDescName().equals(d.getName())) {
							logger.error(msg + "Descriptor " + d + " has a tree vertex " + v
									+ " which doesn't link back to it correctly: treeVertex.getDescName() = "
									+ v.getDescName());
							validFlag = false;
						}
					}
				}
				if (!descName2desc.get(d.getName()).equals(descUi2desc.get(d.getUI()))) {
					logger.error(msg + "Descriptor " + d
							+ " is not link back by descName2desc and descUi2desc correctly");
					validFlag = false;
				}
			}

			// descriptor-mappings
			for (String name : descName2desc.keySet()) {
				Descriptor v = descName2desc.get(name);
				if (!v.getName().equals(name)) {
					logger.error(msg + "invalid name-descriptor mapping in 'descName2desc : 'key = " + name
							+ " : value = " + v.getName() + "'.");
					validFlag = false;
				}
			}
			for (String ui : descUi2desc.keySet()) {
				Descriptor v = descUi2desc.get(ui);
				if (!v.getUI().equals(ui)) {
					logger.error(msg + "invalid ui-descriptor mapping in 'descUi2desc : 'key = " + ui + " : value = "
							+ v.getUI() + "'.");
					validFlag = false;
				}
			}
		} catch (Exception e) {
			logger.error(msg + "unknown exception - tree cannot be valid. Exception message is: \n" + e.getMessage());
			validFlag = false;
		}
		return validFlag;
	}

	/**
	 * Note that the collection returned will contain a special root descriptor. It is equal to
	 * <code>getRootDesc()</code>.
	 * 
	 * @return Returns a Collection containing all descriptors.
	 * 
	 */
	public Collection<Descriptor> getAllDescriptors() {
		return descName2desc.values();
	}

	/**
	 * @Return Returns a list of all descriptors sorted by their height, starting with the smallest heights, thus
	 *         starting with the root descriptor.
	 */
	public List<Descriptor> getAllDescriptorsByHeight() {
		List<Descriptor> allDescList = new ArrayList<Descriptor>(getAllDescriptors());
		Collections.sort(allDescList, new DescriptorHeightComparator(this));
		return allDescList;
	}

	/**
	 * Uncompleted method that is to return the descriptor with lowest height which has a path in terms of tree vertices
	 * to <code>descriptor</code>. The code is almost finished, however must be verified that the returned descriptor is
	 * indeed the desired one. This is not too sure as its not clear in which order JGraphT returns the shortest paths
	 * which are computed in this method.
	 * 
	 * @param descriptor
	 * @return
	 */
	public Descriptor getDescriptorRootWithLowestHeight(Descriptor descriptor) {
		Descriptor ret = null;

		List<TreeVertex> descriptorVertices = descriptor.getTreeVertices();
		List<TreeVertex> meshRootVertices = childVerticesOf(this.root);

		List<DefaultEdge> shortestPath = null;
		for (TreeVertex descVertex : descriptorVertices) {
			for (TreeVertex rootVertex : meshRootVertices) {
				List<DefaultEdge> path = DijkstraShortestPath.findPathBetween(this, descVertex, rootVertex);
				if (path != null && (shortestPath == null || path.size() < shortestPath.size()))
					shortestPath = path;
			}
		}

		System.out.println("From: " + descriptor.getUI());
		if (shortestPath != null && shortestPath.size() > 0) {
			System.out.println(descriptor.getName());
			TreeVertex begin = getEdgeTarget(shortestPath.get(0));
			TreeVertex end = getEdgeSource(shortestPath.get(shortestPath.size() - 1));
			System.out.println("First on path: " + getDescriptorByVertex(begin).getUI());
			System.out.println("Last on path: " + getDescriptorByVertex(end).getUI());
			ret = getDescriptorByVertex(end);
		} else if (shortestPath != null) {
			System.out.println("Is root: " + descriptor.getTreeVertices().iterator().next().getName());
			ret = descriptor;
		} else
			System.out.println("No path found.");

		return ret;
	}

	/**
	 * Returns a shortest sequence of descriptors so that each descriptor is associated with a tree vertex on a shortest
	 * path between the vertices of descriptor1 and those of descriptor2.
	 * 
	 * @param descriptor1
	 * @param descriptor2
	 * @return
	 */
	public List<Descriptor> getShortestDescriptorPath(Descriptor descriptor1, Descriptor descriptor2) {
		List<DefaultEdge> shortestPath = null;

		List<TreeVertex> desc1Vertices = descriptor1.getTreeVertices();
		List<TreeVertex> desc2Vertices = descriptor2.getTreeVertices();

		for (TreeVertex desc1Vertex : desc1Vertices) {
			for (TreeVertex desc2Vertex : desc2Vertices) {
				List<DefaultEdge> path = DijkstraShortestPath.findPathBetween(this, desc1Vertex, desc2Vertex);
				if (shortestPath == null || path.size() < shortestPath.size() && path.size() != 0)
					shortestPath = path;
			}
		}

		List<Descriptor> descriptorPath = new ArrayList<Descriptor>();
		for (int i = 0; i < shortestPath.size(); i++) {
			DefaultEdge edge = shortestPath.get(i);

			TreeVertex src = getEdgeSource(edge);
			descriptorPath.add(getDescriptorByVertex(src));
			if (i == shortestPath.size() - 1) {
				TreeVertex tgt = getEdgeTarget(edge);
				descriptorPath.add(getDescriptorByVertex(tgt));
			}
		}

		return descriptorPath;
	}

	public void renameDescriptorWithUi(String ui, String newName) {
		Descriptor desc = descUi2desc.get(ui);
		if (null == desc)
			return;
		String descName = desc.getName();
		if (null == descName2desc.get(descName))
			throw new IllegalStateException("The descriptor with UI " + ui + " has name " + descName
					+ " but is not found in the name map with this name.");
		desc.setName(newName);
		for (TreeVertex v : desc.getTreeVertices()) {
			v.setDescName(newName);
		}
		descName2desc.remove(descName);
		descName2desc.put(newName, desc);

	}

}