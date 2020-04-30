package de.julielab.concepts.db.creators.mesh.tools;

import java.util.*;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;

/**
 * Utility class for VertexComparator only.
 * 
 * It tries to find out what the origin of a tree-vertex in one tree in another tree was.
 * See also <code>evaluate</code>.
 * 
 * @author Philipp Lucas
 */

public class EvaluationMap {

	@SuppressWarnings("unused")
	private static Logger logger =  org.slf4j.LoggerFactory.getLogger(EvaluationMap.class);
	
	private LinkedHashMap<TreeVertex, Double> evalMap = new LinkedHashMap<TreeVertex, Double>();
	private TreeVertex targetVertex;
	private Tree targetTree, candidateTree;

	/**
	 * Constructor.
	 * 
	 * @param targetVertex
	 *            A tree vertex.
	 * @param candidateDesc
	 * 			  Descriptor whose tree-vertices are to be used as candidates to 'match' <code>targetVertex</code>.           
	 * @param targetTree
	 *            Tree of <code>targetVertex</code>.
	 * @param candidateTree
	 *            Tree in which the origin of <code>targetVertex</code> is to be
	 *            found.
	 */
	public EvaluationMap(TreeVertex targetVertex, Descriptor candidateDesc,
			Tree targetTree, Tree candidateTree) {
		this.targetVertex = targetVertex;
		this.targetTree = targetTree;
		this.candidateTree = candidateTree;

		for (TreeVertex c : candidateDesc.getTreeVertices()) {
			evalMap.put(c, 0.0);
		}
	}

	/**
	 * <p>
	 * This assigns a "goodness-value" to each of the possible origins
	 * <code>o</code> (elements of <code>evalMap</code>) of
	 * <code>targetVertex</code>. "origin" means that <code>o</code> is the vertex
	 * that was moved and became <code>targetVertex</code>.
	 * </p>
	 * <p>
	 * To achieve that the children of each <code>o</code> are compared with the
	 * children of <code>targetVertex</code>. For each matching child of these two
	 * tree-vertices we give one point. We also give one point if they both have
	 * no children. We only go <code>depth</code> many levels down.
	 * </p>
	 * 
	 * @param d
	 *            Depth till which evaluation should be done. If depth == 0
	 *            nothing will be evaluated. If depth == 1 then the direct
	 *            children of <code>ud</code> and each key of
	 *            <code>evalMap</code> will be evaluated. And so on...
	 */
	public void evaluate(int depth) {
		for (TreeVertex origin : evalMap.keySet()) {
			// get new value
			double value = traverseForEval(targetVertex, origin, 1, depth);

			// update evalMap
			evalMap.put(origin, value);
		}
	}

	/**
	 * <p>
	 * This documentation might be inaccurate.
	 * Traverse the source and target tree "in parallel". The currently
	 * traversed vertex in <code>targetTree</code> is <code>curSource</code> and
	 * the one in <code>candidateTree</code> is <code>curTarget</code>. It only
	 * traverses further down to a child vertex <code>v</code> (in source tree)
	 * and <code>w</code> (in target tree) if <code>v</code> and <code>w</code>
	 * match, i.e. both <code>v</code> and <code>w</code> belong to the same
	 * descriptor (i.e. the same UI, not determined by equals method).
	 * </p>
	 * 
	 * <p>
	 * The aim of the method is to calculate a matching value, which is the
	 * higher the more matching tree vertices there are. For each matching
	 * vertex the value increases by 1.
	 * </p>
	 * 
	 * @param curSource
	 *            Currently traversed tree vertex in source tree.
	 * @param curTarget
	 *            Currently traversed tree vertex in target tree.
	 * @param currentDepth
	 *            Current depth.
	 * @param maxDepth
	 *            Maximal depth we traverse down.
	 * @return Returns a matching value.
	 */
	private double traverseForEval(TreeVertex curSource, TreeVertex curTarget,
			int currentDepth, int maxDepth) {
		
		// abort condition
		if (currentDepth > maxDepth) {
			return 0;
		}

		// check match for each child of curSource
		double value = 0;
		for (TreeVertex child : targetTree.childVerticesOf(curSource)) {
			TreeVertex match = VertexComparator.getMatchingChild(curTarget,
					child, candidateTree);
			// add +1 if match, and traverse down only if matched
			if (match != null) {
				value++;
				value += traverseForEval(child, match, currentDepth + 1,
						maxDepth);
			}
		}
		return value;
	}

	/**
	 * Removes all key-entry pairs from <code>evalMap<code> but those with the
	 *  highest value max and up to <code>d</code> less than this.
	 * 
	 * @param evalMap
	 *            A key-value map.
	 * @param d
	 *            Values >= max - d will be kept.
	 */
	public void kickAllButBest(double d) {
		double max = Double.NEGATIVE_INFINITY;

		// get maximum
		Collection<Double> values = evalMap.values();
		for (double v : values) {
			if (v > max) {
				max = v;
			}
		}

		// save all values to delete (those less than max-d)
		max = max - d;
		Collection<Double> toDelete = new LinkedHashSet<Double>();
		for (double v : values) {
			if (v < max) {
				toDelete.add(v);
			}
		}
		
		//logger.debug("number of deleted values : " + toDelete.size());
		
		// delete 
		// note: this sequential approach of "getting maximum, saving those to delete 
		// and then deleting" is necessary to avoid concurrent modification exceptions...
		values.removeAll(toDelete);
		
		
	}

	/**
	 * 
	 * @return Returns the set of all 'matching vertices'. The values returned
	 *         depend on the filter (-> <code>evaluate()</code>) you applied
	 *         before.
	 */
	public Set<TreeVertex> getMatches() {
		return evalMap.keySet();
	}

	/**
	 * @param evalMap
	 *            An evaluation map.
	 * @return Returns the sum of all values in <code>evalMap</code>.
	 */
	public double sumOfAllValues() {
		double sum = 0;
		for (double v : evalMap.values()) {
			sum = sum + v;
		}
		return sum;
	}

	public int size() {
		return evalMap.size();
	}
	
	public boolean isEmpty() {
		return evalMap.isEmpty();
	}
	
	@Override
	public String toString() {
		String tmp = "";
		for(TreeVertex v : evalMap.keySet()) {
			tmp = tmp + v + " = " + evalMap.get(v) + "\n";
		}
		return tmp;
	}
}
