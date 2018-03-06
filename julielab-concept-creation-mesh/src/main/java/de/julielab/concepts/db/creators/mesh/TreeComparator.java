package de.julielab.concepts.db.creators.mesh;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.components.Concept;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.Term;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * Abstract base class for <code>Tree</code> comparison.
 *
 * Implemented by <code>TreeComparatorMeSH<>/code> and <code>TreeComparatorUD</code>.
 * 
 * @author Philipp Lucas
 *
 */
public abstract class TreeComparator extends TreeModificationContainer {
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(TreeComparator.class);
	
	// source and target Tree
	protected Tree source, target;
	
	// progress counter
	protected ProgressCounter counter;
	
	// name of this comparator
	String name;
	
	/**
	 * Constructor. Here we set which two objects of <code>Tree</code> we want
	 * to compare in order to find the set of operations that leads from one to
	 * another. 
	 * 
	 * @param source
	 *            Initial <code>Tree</code>.
	 * @param target
	 */
	public TreeComparator(Tree source, Tree target) {
		super();
		this.source = source;
		this.target = target;
		this.name = "unnamed";
	}
	
	/**
	 * like the other constructor. but we can give a name to the comparator.
	 */
	public TreeComparator(Tree source, Tree target, String name) {
		super();
		this.source = source;
		this.target = target;
		this.name = name;
	}
	
	protected abstract void determineRenamingsAndRebindings();
	
	protected abstract void determineAddsAndMovings();
	
	protected abstract void determineDeletions();
	
	public void determineModifications() {
		logger.info("# Determining modifications for '" + source.getName() + "' -> '" + target.getName() + "' ... ");
		
		determineRenamingsAndRebindings();
		determineAddsAndMovings();
		determineDeletions();
		
		descRenamings.removeUnnecessary();
		descRelabellings.removeUnnecessary();
		vertexRenamings.removeUnnecessary();
		
		logger.info("# ... done determining modifications.");
	}
	
	/**
	 * <p>Returns true iff <code>o1</code> and <code>o2</code> have equal structure. I.e.:
	 * <ul>
	 * <li> they have the same number of tree-vertices </li>
	 * <li> they have the same number of descriptors </li>
	 * <li> all their tree-vertices equals in terms of offspring, vertex-names and corresponding descriptor. </li>
	 * <li> all their descriptor equals in terms of UI and name. </li>
	 * </ul>
	 * In turn we do not take into account descriptor terms, scope notes, etc! </p>
	 * 
	 * <p>
	 * Otherwise, it returns false;  
	 * </p>
	 */
	public static boolean isEqualTrees(Tree o1, Tree o2) {		
		boolean ret = true;
		logger.info("# Checking equality of '" + o1.getName() + "' and '" + o2.getName() + "' ... "); 
		// same nr of descriptors 
		if (o1.getAllDescriptors().size() != o2.getAllDescriptors().size()) {
			logger.info(o1.getAllDescriptors().size() + " != " + o2.getAllDescriptors().size() + " descs.");
			ret = false;
		}
		
		// for counting vertices
		int v1cnt = 0;
		int v2cnt = 0;
		
		// checking each descriptor in o1
		for(Descriptor d1 : o1.getAllDescriptors()) {
			
			String d1Ui = d1.getUI(); 
			String d1Name = d1.getName();
			
			Descriptor d2 = o2.getDescriptorByUi(d1Ui);
			if (d2 == null) {
				logger.info(d1.toString() + " is not part of '" + o2.getName() + "'");
				ret = false;
				
			} else {
				String d2Name = d2.getName();
				
				if(!d2Name.equals(d1Name)) {
					logger.info(d1.toString() + " != " + d2.toString());
					ret = false;
				}
				
				v1cnt += d1.getTreeVertices().size();
				
				// for checking each vertex (of desc in o1)
				List<TreeVertex> v2s = d2.getTreeVertices();
				for(TreeVertex v1 : d1.getTreeVertices()) {
					String v1Name = v1.getName();
					// so ugly... X_x
					boolean contained = false;
					for(TreeVertex v2 : v2s) {
						if (v2.getName().equals(v1Name)) {
							contained = true;
							break;
						}
					}
					
					if (!contained) {
						logger.info("Descriptor " + v1.toString() + " is missing in '" + o2.getName() + "'");
						ret =  false;
					}
				}
			}
		}

		// checking each descriptor in o2
		for(Descriptor d2 : o2.getAllDescriptors()) {

			String d2Ui = d2.getUI(); 
			String d2Name = d2.getName();

			Descriptor d1 = o1.getDescriptorByUi(d2Ui);
			if (d1 == null) {
				logger.info(d2.toString() + " is not part of '" + o1.getName() + "'");
				ret = false;

			} else {
				String d1Name = d1.getName();

				if(!d1Name.equals(d2Name)) {
					logger.info("d2 " + d2.toString() + " != d1 " + d1.toString());
					ret = false;
				}

				v2cnt += d2.getTreeVertices().size();
				
				// for checking each vertex (of desc in o1)
				List<TreeVertex> v1s = d1.getTreeVertices();
				for(TreeVertex v2 : d2.getTreeVertices()) {
					String v2Name = v2.getName();
					// so ugly... X_x
					boolean contained = false;
					for(TreeVertex v1 : v1s) {
						if (v1.getName().equals(v2Name)) {
							contained = true;
							break;
						}
					}

					if (!contained) {
						logger.info(v2.toString() + " is missing in '" + o1.getName() + "'");
						ret =  false;
					}
				}
			}
		}
		
		if (v2cnt != v1cnt) {
			logger.info("number of vertices doesn't match : '" + v1cnt + " for '" + o1.getName() + "' vs " + v2cnt + " for '" + o2.getName() + "'");
			ret = false;
		}
		
		logger.info("# ... done checking equality.");
		return ret;
	}

	/**
	 * TODO: undo static?
	 * @return <p>
	 *         Returns a collection of all those terms which are part of
	 *         <code>data1</code> but not of <code>data2</code>.
	 *         </p>
	 *         <p>
	 *         They are compared on a descriptor level, so only differences
	 *         between descriptors with same name and UI are noticed. Also,
	 *         terms are regarded as equal if their names are equal. UIs are not
	 *         taken into account.
	 *         </p>
	 */
	static public Collection<String> getDeletedTerms(Tree data1, Tree data2) {
		Collection<String> delTerms = new LinkedList<String>();
		for (Descriptor desc1 : data1.getAllDescriptors()) {
			Descriptor desc2 = data2.getDescriptorByUi(desc1.getUI());
			if (desc2 != null) {
				// build term sets
				Set<String> termSet1 = new HashSet<String>();
				for (Concept c : desc1.getConcepts()) {
					for (Term t : c.getTerms()) {
						termSet1.add(t.getName());
					}
				}
				Set<String> termSet2 = new HashSet<String>();
				for (Concept c : desc2.getConcepts()) {
					for (Term t : c.getTerms()) {
						termSet2.add(t.getName());
					}
				}
				// check termSet1 against termSet2
				for (String name1 : termSet1) {
					if (!termSet2.contains(name1)) {
						delTerms.add(name1);
					}
				}
			}
		}
		return delTerms;
	}
	
	@Override
	public String toString() {
		StringBuffer out = new StringBuffer();
		out.append("## Comparator '"+ name + "'\n");
		out.append("# Descriptor - Additions : \n"); 
		out.append(getDescAdditions().toString());
		out.append("# Descriptor - Deletions : \n"); 
		out.append(getDescDeletions().toString());
		out.append("# Descriptor - Renamings :\n"); 
		out.append(getDescRenamings().toString());
		out.append("# Vertex - Additions : \n"); 
		out.append(getVertexAdditions().toString());
		out.append("# Vertex - Deletions : \n"); 
		out.append(getVertexDeletions().toString());
		out.append("# Vertex - Movings: \n");
		out.append(getVertexMovings().toString());
		return out.toString();
	}
	
	/**
	 * Note: it will <i>not</i> modify <code>source</code> or
	 * <code>target</code>.
	 * 
	 * @return <p>
	 *         Returns true if the current modifications applied to
	 *         <code>source</code> will result in a tree that equals
	 *         <code>target</code>. To check if two trees are equal
	 *         <code>isEqualTrees</code> is used.
	 *         </p>
	 */
	public boolean verify() {
		// TODO implement
		return true;
	}
}
