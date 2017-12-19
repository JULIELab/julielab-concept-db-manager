package de.julielab.semedico.mesh.tools;

import java.util.Iterator;

import org.slf4j.Logger;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.TreeVertex;

/**
 * Class to find similar or matching vertices. Not a comparator of vertices in the original sense.   
 * 
 * @author Philipp Lucas
 *
 */
public class VertexComparator {
	
	@SuppressWarnings("unused")
	private static Logger logger =  org.slf4j.LoggerFactory.getLogger(VertexComparator.class);

	/**
	 * Determines the most similar vertex to a given one.
	 * 
	 * @param targetVertex
	 *            The tree vertex to be compared to.
	 * @param candidateDesc
	 *            A descriptor. Its tree vertices are the candidates for the origin of <code>targetVertex</code>.
	 * @param targetTree
	 *            Tree of <code>targetVertex</code>.
	 * @param candidateTree
	 *            Tree to find a similar tree vertex in.
	 * @return Of all tree-vertices of <code>candidateDesc</code> it returns the
	 *         most similar tree vertex to vertex <code>targetVertex</code> in
	 *         <code>candidateTree</code> in terms of similarity of the offspring.
	 */
	public static TreeVertex getMostSimilarVertex(TreeVertex targetVertex,
			Descriptor candidateDesc, Tree targetTree, Tree candidateTree) {

		EvaluationMap evalMap = new EvaluationMap(targetVertex, candidateDesc, targetTree, candidateTree);
		
//		if ( evalMap.isEmpty()) {
//			logger.error("evalMap.size == 0!");
//			System.exit(1);
//		}  else 
//		if (evalMap.size() > 1) {
//			logger.debug("attention !");
//			logger.debug("evalMap.size (1) == " + evalMap.size());
//		}

		// go deeper till we have a decision
		// = the sum of all values and number of values in the evaluation didn't change
		// -> that implies we didn't get down any further / no candidates were thrown
		double oldSum = -1;
		double curSum = evalMap.sumOfAllValues();
		int oldSize = evalMap.size();
		int curSize = evalMap.size();

		for (int d = 1; (curSize > 1) && ((oldSum != curSum) || (oldSize != curSize)); d++) {
			evalMap.evaluate(d);
			evalMap.kickAllButBest(0);
			
			oldSum = curSum;
			curSum = evalMap.sumOfAllValues();
			oldSize = curSize;
			curSize = evalMap.size();
		}
		
		//logger.debug("evalMap.size (2) == " + evalMap.size());

		// if there is more than one candidate left we got a problem... for now, just take the first one.	
		Iterator<TreeVertex> iter = evalMap.getMatches().iterator();
		TreeVertex origin = iter.next();
		
//		if(iter.hasNext()) {			
//			logger.warn(" when restoring I chose " + origin + " from : ");
////			while (iter.hasNext()) {
////				logger.warn("    " + iter.next().toString());
////			}
//			logger.warn(evalMap.toString());
//		}

		return origin;
	}

	/**
	 * Finds a matching vertex. If <code>parent</code> has a child whose
	 * respective descriptor UI matches the respective descriptor UI of
	 * <code>toMatch</code> this child is returned.
	 * 
	 * @param parent
	 *            A vertex whose children will be tested for a match.
	 * @param toMatch
	 *            The vertex that needs to be matched.
	 * @param data
	 *            Tree instance that contains <code>parent</code>.
	 * @return Returns the matching vertex; <code>null</code> else.
	 */
	public static TreeVertex getMatchingChild(TreeVertex parent,
			TreeVertex toMatch, Tree data) {
		// get the descriptor UI that needs to match
		String toMatchDescUi = toMatch.getDescUi();
		// try all children of parent if they match -> return first match
		for (TreeVertex child : data.childVerticesOf(parent)) {
			if (child.getDescUi().equals(toMatchDescUi)) {
				return child;
			}
		}
		return null;
	}

}
