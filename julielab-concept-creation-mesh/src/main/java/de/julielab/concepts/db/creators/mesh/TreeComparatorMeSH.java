package de.julielab.concepts.db.creators.mesh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;

import com.google.common.collect.Ordering;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.components.VertexLocations;
import de.julielab.concepts.db.creators.mesh.modifications.VertexDeletions;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;
import de.julielab.concepts.db.creators.mesh.tools.ValueComparableMap;
import de.julielab.concepts.db.creators.mesh.tools.VertexComparator;

/**
 * <p>
 * This compares two instances of the MeSH and determines the modifications that lead from <code>source</code> to
 * <code>target</code>.
 * </p>
 * 
 * <p>
 * Some notes about the determined operations:
 * <ul>
 * <li>in general two runs might lead to different results. This is because there is no garantuee that the tree is
 * traversed in the same order each time, thus possibly leading to different results.</li>
 * <li><code>DescAdditions</code> allows to store vertex additions in new descriptors as well, however for the sake of
 * minimal ambiguity, we chose to store all vertex additions in <code>VertexAdditions</code>.</li>
 * <li>If there is an descriptor deletion, the corresponding vertex deletions are always separately stored in
 * <code>VertexDeletions</code> as well.</li>
 * <li>vertex renamings and descriptor renamings includes <i>all</i> changes of vertex names and descriptor UIs,
 * respectively. That includes those resulting from moving. Thus, vertex renamings and descriptor renamings indicate
 * whether a vertex and descriptor got substantially changed or not.</li> *
 * </ul>
 * </p>
 * 
 * <p>
 * <b>Implementation notes:</b>
 * <ul>
 * <li>vertex renamings and descriptor renamings key sets include a key for <i>every</i> vertex and descriptor in
 * <code>target</code>. This is because they are used as a uniform way to access the name of a vertex / UI of a
 * descriptor in <code>source</code> by its name / UI in target. Thus, also for names / UIs that didn't change or refer
 * to a new vertex / descriptor, there is a renaming from its name / UI to the same name / UI.</li>
 * <li>vertex renamings also track which vertices have been processed already - in <code>source</code> and in
 * <code>target</code>! We can check <code>vertexRenamings.containsOld()</code> to find out whether a certain vertex in
 * <code>source</code> was found to be the equivalent vertex in <code>target</code>. And we can check
 * <code>vertexRenamings.containsNew()</code> to find out whether we already processed a tree vertex of
 * <code>target</code></li>
 * <li>these unnecessary renamings are removed in the end of the comparison process.</li>
 * <li>
 * <code>!vertexRenamings.containsOld(v_source.getName())</code> is the typical statement in
 * <code>restoreAddsAndMovings(TreeVertex v_target)</code> to check a candidate modification. We may not overwrite any
 * previously made renamings, since we would have to find a new origin for the vertex that was the new name of the
 * renaming that would be overwritten. Or in short: overwriting a renaming means deleting a previously determined
 * modification. However, we need to determine exactly one modification for each call of
 * <code>restoreAddsAndMovings(TreeVertex v_target)</code>.
 * </ul>
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */
public class TreeComparatorMeSH extends TreeComparator {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(TreeComparatorMeSH.class);

	/**
	 * Constructor. Here we set which two objects of <code>Tree</code> we want to compare in order to find the set of
	 * operations that leads from one to another.
	 * 
	 * @param source
	 *            'Source' <code>Tree</code>.
	 * @param target
	 *            'Target' <code>Tree</code>.
	 */
	public TreeComparatorMeSH(Tree source, Tree target) {
		super(source, target);
	}

	/**
	 * Like the other constructor. but we can give a name to the comparator.
	 * 
	 * @param source
	 *            'Source' <code>Tree</code>.
	 * @param target
	 *            'Target' <code>Tree</code>.
	 * @param name
	 *            Name of this comparator.
	 */
	public TreeComparatorMeSH(Tree source, Tree target, String name) {
		super(source, target, name);
	}

	/**
	 * Determines renamings of descriptors, rebindings of vertices and all new descriptors.
	 * 
	 * <p>
	 * Note: it does not detect new vertices of new descriptors. <code>determineAddsAndMovings</code> takes care of
	 * this.
	 * </p>
	 * 
	 * <p>
	 * Note: one case of rebinding is not detected here: vertex got moved + rebound to another descriptor. However,
	 * <code>determineAddsAndMovings</code> takes care of this.
	 * </p>
	 */
	@Override
	public void determineRenamingsAndRebindings() {
		/*
		 * 1. step: determine descriptor additions, renamings and relabellings
		 */
		for (Descriptor desc : target.getAllDescriptors()) {

			String targetDescName = desc.getName();
			String targetDescUi = desc.getUI();
			Descriptor sourceDesc = source.getDescriptorByUi(targetDescUi);
			if (sourceDesc == null) {

				// we got a new UI - but also a new name & there is no desc with same name?
				if (!source.hasDescriptorByName(targetDescName)) {

					/*
					 * new descriptors
					 */
					descAdditions.put(desc, new VertexLocations());
					descRenamings.put(targetDescUi, targetDescUi);
				}

				else {
					/*
					 * renaming of a descriptor - UI changed
					 */
					String sourceDescUi = source.getDescriptorByName(targetDescName).getUI();

					/*
					 * check that there is no descriptor with that UI in target: Scenario with problems: - there is 'D01
					 * - Test' in source - there is 'D02 - Test' in target - there is 'D01 - Test2' in target What
					 * happens: - 1. 'D02' doesn't exist in source -> we get here - 2. Test of 'D02' refers to 'D01' in
					 * source - 3. However, there is actually a desc 'D01' in target - just with another name ('Test2')
					 */
					if (!target.hasDescriptorByUi(sourceDescUi)) {
						descRenamings.put(sourceDescUi, targetDescUi);
					} else {
						// otherwise it is a new descriptor
						descAdditions.put(desc, new VertexLocations());
						descRenamings.put(targetDescUi, targetDescUi);
						// note: we don't care about the new 'name' of 'D01' in the above scenario!
					}
				}
			}
			// so descriptor with that UI exists. But does it have the same name?
			else {
				descRenamings.put(targetDescUi, targetDescUi);
				if (sourceDesc.getName().equals(targetDescName)) {
					// descriptor didn't get changed -> nothing to note here
				} else {
					// descriptor got same UI but different name
					descRelabellings.put(sourceDesc.getName(), targetDescName);
				}
			}
		}

		/*
		 * 2. step: determine vertex rebindings note: we need to know about all descriptor renamings already!
		 */
		for (Descriptor desc : target.getAllDescriptors()) {

			// we check all those tree vertices (of the current desc) which did NOT get moved (i.e. their tree-number
			// stayed the same)
			for (TreeVertex cur : desc.getTreeVertices()) {
				TreeVertex prev = source.getVertex(cur.getName());

				// prev == null iff source doesn't contain prev
				if (prev != null) {
					String prevDescName = prev.getDescName();
					String prevDescUI = prev.getDescUi();
					String curDescName = cur.getDescName();
					String curDescUI = cur.getDescUi();

					if (prevDescUI.equals(curDescUI) && !prevDescName.equals(curDescName)) {
						/*
						 * preferred term of descriptor changed - but we don't care about that
						 */

					} else if (!prevDescUI.equals(curDescUI) && prevDescName.equals(curDescName)) {
						/*
						 * renaming of a descriptor - UI changed but this is taken care of independently of tree
						 * vertices above already.
						 */

					} else if (!prevDescUI.equals(curDescUI) && !prevDescName.equals(curDescName)) {

						/*
						 * rebinding of vertex to another descriptor. But: is it really rebinding, or is it just a
						 * renaming of a descriptor? Note: we don't need to add vertexRenamings here because this would
						 * be a renaming for a vertex that doesn't change. And these renamings are take care of in
						 * <code>determineAddsAndMovings</code>.
						 */
						if (!descRenamings.contains(prevDescUI, curDescUI)) {
							vertexMovings.put(cur.getName(), source.parentVertexOf(prev).getName(), target
									.parentVertexOf(cur).getName(), prevDescUI, curDescUI);
						}
					}
				}
			}
		}
	}

	@Override
	public void determineAddsAndMovings() {
		logger.info("# Traversing '" + target.getName() + "' to determine additions and movings ... ");
		counter = new ProgressCounter(source.vertexSet().size(), 10, "tree-vertex");
		List<TreeVertex> orderedVertexList = new ArrayList<>(target.vertexSet());
		Collections.sort(orderedVertexList);
		// pre-processing: mark all unmoved vertices as such!
		// for(TreeVertex v : target.vertexSet()) {
		for (TreeVertex v : orderedVertexList) {
			String vName = v.getName();
			if (source.hasVertex(vName)) {
				vertexRenamings.put(vName, vName);
			}
		}

		// traverse tree to find out modifications
		traverse4AddsAndMovings(target.getRootVertex());

		counter.finishMsg();
		logger.info("# ... done determining additions and movings");
	}

	private void traverse4AddsAndMovings(TreeVertex cur) {
		for (TreeVertex child : target.childVerticesOf(cur)) {
			restoreAddsAndMovings(child);
			counter.inc();
			traverse4AddsAndMovings(child);
		}
	}

	/**
	 * <p>
	 * General idea to find the origin of a vertex in <code>target</code>: find tree-vertex in <code>source</code> with
	 * similar offspring which doesn't exist anymore. Also, take into account renamings of descriptors (determined
	 * beforehand) and vertices (determined here).
	 * <p>
	 * 
	 * <p>
	 * As a general rule, we assume that for all ancestors of <code>v_target</code> we successfully determined the
	 * corresponding operations (if any). This is particularly important as we need to know the corresponding vertex of
	 * <code>v_target</code>s parent in <code>source</code>.
	 * </p>
	 * 
	 * @param vertex
	 *            A tree vertex in <code>target</code>.
	 */
	private void restoreAddsAndMovings(TreeVertex v_target) {
		/*
		 * case 1 : v_target didn't change at all: there can only be additions or movings to vertices if the tree-number
		 * changed.
		 */
		if (source.hasVertex(v_target.getName())) {
			return;
		}

		// naming conventions:
		// *_target = * in target tree.
		// *_source = * in source tree.
		TreeVertex parent_target = target.parentVertexOf(v_target);
		String descUi_target = v_target.getDescUi();
		Descriptor desc_source;

		/*
		 * case: descriptor of v_target is somehow modified. So it is new, got renamed or relabelled.
		 */
		if (!source.hasDescriptorByUi(descUi_target)) {

			/*
			 * once we get here, question is: do we got a new vertex as well or a moved, rebound vertex? Idea for
			 * solving this problem: check if for offspring of v_target it holds that their parents in source are
			 * similar to v_target. If they are then v_target got moved (unsolved though: from where?). If not, v_target
			 * is new. So, we store in a map how often a vertex (in source) was found to be the parent of the children
			 * of v_target
			 */

			// we use a fancy map that sorts reversed by value!
			TreeMap<String, Integer> candidates = new ValueComparableMap<String, Integer>(Ordering.natural().reverse());
			for (TreeVertex child_target : target.childVerticesOf(v_target)) {
				// we assume that the children of v_target are unchanged...! (of course their name changed, though!)
				Descriptor childDesc_source = source.getDescriptorByUi(descRenamings.getOld(child_target.getDescUi()));
				if (childDesc_source == null) {
					continue;
				}
				TreeVertex child_source = VertexComparator.getMostSimilarVertex(child_target, childDesc_source, target,
						source);
				String childsParentName_source = source.parentVertexOf(child_source).getName();

				// increase count by 1
				Integer cnt = candidates.get(childsParentName_source);
				if (cnt == null) {
					cnt = new Integer(1);
				} else {
					cnt++;
				}
				candidates.put(childsParentName_source, cnt);
			}

			// try best matches one after another (best = value highest)
			for (String childsParentName_source : candidates.keySet()) {
				/*
				 * Check that candidate is actually the origin of v_target ... but how? Really don't know how to confirm
				 * that, since tree-number and desc of v_target changed. Only the offspring might(!) be the same - and
				 * that's what we analyzed already. However, at least there should be 2 vertices pointing to the same
				 * parent, since 1 is not a hint for anything as any single child will cause a count of 1 for its
				 * parent.
				 */

				// check that the candidate origin is not existing in target...
				if (target.hasVertex(childsParentName_source)) {
					continue;
				}

				/*
				 * case 3.2: new descriptor, but no new vertex - vertex just got moved here. we found the
				 * <code>v_source</code> to be the current candidate.
				 */
				if (candidates.get(childsParentName_source) <= 1) {
					continue;
				}

				TreeVertex v_source = source.getVertex(childsParentName_source);
				TreeVertex parent_source = source.parentVertexOf(v_source);

				// check that there is not any moving for <code>v_source</code> already.
				// this is in fact just an ugly workaround ... see "Studienarbeit: Outlook - Ausblick"
				if (vertexRenamings.containsOld(v_source.getName())) {
					continue;
				}

				vertexMovings.put(v_source.getName(), parent_source.getName(), parent_target.getName(),
						v_source.getDescUi(), descUi_target);
				vertexRenamings.put(v_source.getName(), v_target.getName());
				return;
			}

			/*
			 * case 3.1: new vertex. Note: we only get here if all the candidates didn't suffice...
			 */
			/*
			 * What's the parent? -> depends on whether the parent in target is a new vertex, renamed vertex or
			 * unchanged vertex. this is saved in vertexRenamings...
			 */
			vertexAdditions.put(v_target.getName(), vertexRenamings.getOld(parent_target.getName()), descUi_target);
			vertexRenamings.put(v_target.getName(), v_target.getName());
			return;
		}

		// if we get till here: descriptor of v_target is present in source (and unchanged)

		// assuming v_target got moved: where did it come from?
		desc_source = source.getDescriptorByUi(descUi_target);
		TreeVertex v_source = VertexComparator.getMostSimilarVertex(v_target, desc_source, target, source);
		TreeVertex parent_source = source.parentVertexOf(v_source);

		/*
		 * case 4.1 implicit movings: When a ancestor of a vertex v is moved, then v's tree-nr is changed as well.
		 * that's why we will find movings, which are none. So we check if the found moving actually does more than just
		 * changing the name of the vertex.
		 */
		boolean parentsEqual = vertexRenamings.getOld(parent_target.getName()).equals(parent_source.getName());
		boolean descsEqual = desc_source.getUI().equals(descUi_target);
		boolean existsFlag = vertexRenamings.containsOld(v_source.getName());
		if (parentsEqual && descsEqual && !existsFlag) {
			vertexRenamings.put(v_source.getName(), v_target.getName());
			return;
		}

		/*
		 * case 4.2 explicit moving: that's the case if the source vertex of the 'moving' does not exist in target
		 * anymore.
		 * 
		 * Note: !vertexRenamings.containsOld(v_source.getName()) is to avoid overwriting of existing movings.
		 */
		if (!target.hasVertex(v_source.getName()) && !vertexRenamings.containsOld(v_source.getName())) {

			/*
			 * Note: !source.isAnchestorVertex(v_source.getName(), old_p_target) to avoid invalid movings should somehow
			 * in another way take care of this...
			 */
			TreeVertex old_p_target = source.getVertex(vertexRenamings.getOld(parent_target.getName()));
			if (old_p_target != null && !source.isAnchestorVertex(v_source.getName(), old_p_target)) {
				vertexMovings.put(v_source.getName(), parent_source.getName(),
						vertexRenamings.getOld(parent_target.getName()), desc_source.getUI(), descUi_target);
				vertexRenamings.put(v_source.getName(), v_target.getName());
				return;
			}
		}

		/*
		 * case 5: new vertex of existing desc. If source vertex of the 'moving' does exist in target.
		 */
		// Note: we need add it as a child of parent's name in source, since
		// we first apply additions, then movings, then deletions.
		vertexAdditions.put(v_target.getName(), vertexRenamings.getOld(parent_target.getName()), v_target.getDescUi());
		vertexRenamings.put(v_target.getName(), v_target.getName());
		return;

	}

	/**
	 * Determines deletions of vertices and descriptors.
	 * 
	 * <p>
	 * A vertex is deleted if it's existing in <code>source</code> but not in <code>target</code> anymore and if it
	 * wasn't moved.
	 * </p>
	 * <p>
	 * A descriptor is deleted if it's existing in <code>source</code> but not in <code>target</code> anymore and if it
	 * relabelled. Note that empty descriptors are thus not regarded to be deleted.
	 * </p>
	 */
	@Override
	public void determineDeletions() {
		// detect vertex deletions
		for (Descriptor desc : source.getAllDescriptors()) {
			for (TreeVertex cur : desc.getTreeVertices()) {
				String curName = cur.getName();
				if (!target.hasVertex(curName) && !vertexMovings.contains(curName)
						&& !vertexRenamings.containsOld(curName)) {
					vertexDeletions.put(curName, VertexDeletions.SINGLE);
				}
			}
		}
		vertexDeletions.detectRecursiveDeletions(source);
		vertexDeletions.updateAdditonalDescriptorInfos(source);

		// detect descriptor deletions
		for (Descriptor desc : source.getAllDescriptors()) {
			String descUi = desc.getUI();
			if (!target.hasDescriptorByUi(descUi) && !descRenamings.containsOld(descUi)) {
				descDeletions.add(descUi);
			}
		}

	}

}
