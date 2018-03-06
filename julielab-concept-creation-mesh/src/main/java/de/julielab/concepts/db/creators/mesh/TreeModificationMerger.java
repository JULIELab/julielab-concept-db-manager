package de.julielab.concepts.db.creators.mesh;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.components.VertexLocations;
import de.julielab.concepts.db.creators.mesh.modifications.DescAdditions;
import de.julielab.concepts.db.creators.mesh.modifications.DescDeletions;
import de.julielab.concepts.db.creators.mesh.modifications.DescRelabellings;
import de.julielab.concepts.db.creators.mesh.modifications.DescRenamings;
import de.julielab.concepts.db.creators.mesh.modifications.VertexAdditions;
import de.julielab.concepts.db.creators.mesh.modifications.VertexDeletions;
import de.julielab.concepts.db.creators.mesh.modifications.VertexMovings;
import de.julielab.concepts.db.creators.mesh.modifications.VertexRenamings;

/**
 * <p>
 * This class is able to create a set of 'similar' modifications for a modified
 * version of a <code>Tree</code> object from a set of original modifications,
 * which were made for the original version of the same <code>Tree</code>
 * object.
 * </p>
 * 
 * <p>
 * Background is the MeSH, which changes every year. However, once a set of
 * modifications was devised to create the Semedico MeSH from the original MeSH.
 * Thus, when applying the same set on a updated version of the MeSH, it might
 * not work. Here, this class comes into play. It receives the former and
 * current version of the MeSH <code>source</code> and <code>target</code> (or
 * in fact <i>any</i> two <code>Tree</code> objects), and a set of operation
 * <code>modSource</code> to be applied on <code>source</code>. It will then
 * determine a new set of modifications <code>modTarget</code> that can be
 * successfully applied on <code>target</code>. It will thereby try to keep the
 * semantics of <code>modSource</code> as much as possible.
 * </p>
 * 
 * <p>
 * <b> Notes about order of fixing modifications: </b> When checking for fixes,
 * we need often have to take into account the changes that are introduced by
 * <code>modSource</code> For example see <code>fixDescDeletion</code>. There we
 * check that
 * <code>(!target.hasDescriptorByUi(descUi) && !descAdd_source.containsByUI(descUi))</code>
 * . We need to make sure that we do the fixings in an order that guarantees
 * that when we check e.g. for descriptor addition we already determined
 * <i>all</i> descriptor additions. Thus the order is the order we use.
 * </p>
 * 
 * <p>
 * Note: It is * unlikely that we want to delete a descriptor that we add just
 * with the same set of modifications, but for the sake of completeness this is
 * necessary. However, that is not the point here).
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */
public class TreeModificationMerger {

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(TreeModificationMerger.class);

	// source and target Tree object
	private Tree source, target;

	// stores modifications to be applied on target. i.e. this is what we want to determine
	private TreeModificator modTarget;

	// stores modifications to be applied on source. i.e. this is what we get as an input
	private TreeModificator modSource;

	// comparator to find modifications leading from source to target  
	private TreeComparator source2target;
	
	// for counting the number of modifications processed and the number of fixes applied
	private int cntMod = 0;
	private int cntFix = 0;	
	
	// for convenient access to modifications - reflected in the comparator/modificator objects above.
	DescAdditions descAdd_source;
	DescAdditions descAdd_target = new DescAdditions();
	DescAdditions descAdd_s2t;
	
	VertexAdditions vertexAdd_source;
	VertexAdditions vertexAdd_target= new VertexAdditions();
	VertexAdditions vertexAdd_s2t;
	
	VertexMovings vertexMov_source;
	VertexMovings vertexMov_target = new VertexMovings();
	VertexMovings vertexMov_s2t;
	
	DescRenamings descRen_source;
	DescRenamings descRen_target = new DescRenamings();
	DescRenamings descRen_s2t; 
	
	DescRelabellings descRel_source;
	DescRelabellings descRel_target = new DescRelabellings();
	DescRelabellings descRel_s2t; 
	
	VertexDeletions vertexDel_source;
	VertexDeletions vertexDel_target = new VertexDeletions();
	VertexDeletions vertexDel_s2t;

	DescDeletions descDel_source;
	DescDeletions descDel_target = new DescDeletions();
	DescDeletions descDel_s2t;
	
	VertexRenamings vertexRe_source;
	VertexRenamings vertexRe_target = new VertexRenamings();
	VertexRenamings vertexRe_s2t;
	
	/**
	 * 
	 * @param source
	 *            <code>Tree</code> object on which
	 *            <code>modSource</object> can be applied.
	 * @param target
	 *            <code>Tree</code> object to find a valid set of modifications
	 *            for.
	 * @param modSource
	 *            Modifications for <code>source</code>. Note that subsequent
	 *            modifications to <code>modSource</code> will be reflected in
	 *            this object as well!
	 */
	// 								(mesh2008, mesh2012, mods4semedico2008);
	public TreeModificationMerger(Tree source, Tree target, TreeModificationContainer modSource) {
		// create tree modificator object to be applied on source / target
		this.modSource = new TreeModificator(source,"mods4source");
		this.modTarget = new TreeModificator(target,"mods4target");
		
		// get modifications for source from parametes
		this.modSource.putModification(modSource);
		
		this.source = source;
		this.target = target;
		
		source2target = new TreeComparatorMeSH(source, target, "sourc2target");	
	}


	/**
	 * Does the merging and then returns either the merged modifications or <code>null</code>
	 * if there was an error. Check the logger for detailed error information.
	 */
	public TreeModificator merge() {
		// verify modSource
		if (!modSource.verify()) {
			logger.error("merge() : modSource cannot be applied on Tree " + source.getName() + " - aborting merge!");
			return null;
		}

		// verify source and target
		if (!source.verifyIntegrity() || !target.verifyIntegrity()) {
			return null;
		}
		
		// determine modifications: source -> target
		source2target.determineModifications();

		// verify modTarget
		if (!source2target.verify()) {
			logger.error("merge() : couldn't determine modifications from source ("
					+ source.getName()
					+ ") to target ("
					+ target.getName()
					+ "). Aborting merge.");
			return null;
		}
		
		// merge modifications
		logger.info("# Merging modifications '" + modSource.getName() + "' of '" + source.getName() + "' targeting: '"+ target.getName() +"' ... ");
		copyAndFix();
		
		// verify modTarget
		if (!modTarget.verify()) {
			logger.error("merge() : created modTarget but it's not valid for Tree " + target.getName());
			return null;
		} else {
			logger.info("# ... done merging modification. Approx " + cntFix + " of " + cntMod + " modifications needed fixing.");
			return modTarget;
		}
	}

	/**
	 * Creates the modified version of <code>modSource</code>, i.e.
	 * <code>modTarget</code>, by copying non-conflicting modifications and
	 * fixing conflicting modifications.
	 */
	private void copyAndFix() {
		int cntTmp;
		
		descRen_source = modSource.getDescRenamings();
		descRen_s2t = source2target.getDescRenamings(); 
		
		descRel_source = modSource.getDescRelabellings();
		descRel_s2t = source2target.getDescRelabellings(); 
		
		descAdd_source = modSource.getDescAdditions();
		descAdd_s2t  = source2target.getDescAdditions();
		
		vertexAdd_source = modSource.getVertexAdditions();
		vertexAdd_s2t = source2target.getVertexAdditions();
		
		vertexMov_source = modSource.getVertexMovings();
		vertexMov_s2t = source2target.getVertexMovings();
		
		vertexDel_source = modSource.getVertexDeletions();
		vertexDel_s2t = source2target.getVertexDeletions();

		descDel_source = modSource.getDescDeletions();
		descDel_s2t = source2target.getDescDeletions();
		
		vertexRe_source = modSource.getVertexRenamings();
		vertexRe_s2t = source2target.getVertexRenamings();
		
		// for fixing (vertex additions, movings and deletions) we need all vertex deletions explicitly 
		vertexDel_s2t.expandRecursiveDeletions(source);
		
		cntTmp = cntFix;
		for(Descriptor desc : descAdd_source.keySet()) {
			fixDescAddition(desc, descAdd_source.get(desc));
		}
		cntMod+=descAdd_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + descAdd_source.size() + " descAdds.");
		
		cntTmp = cntFix;
		for (String oldUi : descRen_source.getOldSet()) {
			fixDescRenaming(oldUi, descRen_source.getNew(oldUi));
		}
		cntMod+=descRen_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + descRen_source.size() + " descRens.");

		cntTmp = cntFix;
		for (String oldName : descRel_source.getOldSet()) {
			fixDescRelabelling(oldName, descRel_source.getNew(oldName));
		}
		cntMod+=descRel_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + descRel_source.size() + " descRels.");

		cntTmp = cntFix;
		for (String vName : vertexAdd_source.keySet()) {
			fixVertexAddition(vName,
					vertexAdd_source.getParentVertexName(vName),
					vertexAdd_source.getDescUi(vName));
		}
		cntMod+=vertexAdd_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + vertexAdd_source.size() + " vertexAdds.");
		
		cntTmp = cntFix;
		for (String vName : vertexMov_source.keySet()) {
			fixVertexMoving(vName, 
					vertexMov_source.getOldParent(vName), 
					vertexMov_source.getNewParent(vName), 
					vertexMov_source.getOldDescUi(vName), 
					vertexMov_source.getNewDescUi(vName));
		}
		cntMod+=vertexMov_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + vertexMov_source.size() + " vertexMovs.");

		cntTmp = cntFix;
		for (String vName : vertexDel_source.keySet()) {
			fixVertexDeletion(vName, vertexDel_source.get(vName));
		}
		cntMod+=vertexDel_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + vertexDel_source.size() + " vertexDels.");

		
		cntTmp = cntFix;
		for (String descUi : descDel_source) {
			fixDescDeletion(descUi);
		}
		cntMod+=descDel_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + descDel_source.size() + " descDels.");

		
		cntTmp = cntFix;
		for (String oldName : vertexRe_source.getOldSet()) {
			fixVertexRenaming(oldName, vertexRe_source.getNew(oldName));
		}
		cntMod+=vertexRe_source.size();
		logger.info("# fixed ~ " + (cntFix - cntTmp) + " of " + vertexRe_source.size() + " vertexRens.");
		
		// Summarise deletions again
		vertexDel_s2t.detectRecursiveDeletions(source);
		
		modTarget.putModification(descAdd_target);
		modTarget.putModification(descRen_target);
		modTarget.putModification(descRel_target);
		modTarget.putModification(vertexAdd_target);
		modTarget.putModification(vertexMov_target);
		modTarget.putModification(vertexDel_target);
		modTarget.putModification(descDel_target);
		modTarget.putModification(vertexRe_target);		
	}
	

	private void fixDescAddition(Descriptor desc, VertexLocations locs) {
		// addition of descriptor itself
		// fix: do not add descriptor if there is already one with its name or UI. Just add new vertices (if any)
		if (!target.hasDescriptorByName(desc.getName())) {
			if(!target.hasDescriptorByUi(desc.getUI())) {
				descAdd_target.put(desc, new VertexLocations());	
			}
			else {
				logger.warn("Fixing descriptor addition ('" + desc.toString() + "') : UI already there. Just adding tree-vertices");
				cntFix++;
			}
		} else {
			logger.warn("Fixing descriptor addition ('" + desc.toString() + "') : Descriptor name alreadz there. Just adding tree-vertices");
			cntFix++;
		}

		// addition of tree-vertices
		for(String vName : locs.getVertexNameSet()) {
			// we shouldn't call fixVertexAddition at this state! Note that we
			// add it to vertexAdd_SOURCE instead. These additions will be
			// processed later.
			// fixVertexAddition(vName, locs.get(vName),desc.getUI());
			vertexAdd_source.put(vName, locs.get(vName),desc.getUI());
		}
		
		// clear locations
		 locs.clear();		
	}
	
	
	private void fixDescRenaming(String oldUi, String newUi) {
		// oldUi ...
		if (!target.hasDescriptorByUi(oldUi) && !descAdd_source.containsByUI(oldUi)) {
			
			// ... got deleted
			if ( descDel_s2t.contains(oldUi)) {
				// fix: no fix! thanks for the work!
				logger.error("Cannot fix desc renaming of '" + oldUi + "' : it got deleted.");
				return;
			} 
			
			// ... got renamed
			else if ( descRen_s2t.containsOld(oldUi)) {
				// fix : rename the renamed desc 
				oldUi = descRen_s2t.getNew(oldUi);
				fixDescRenaming(oldUi, newUi);
				cntFix++;
				return;
			}
			
			// error!
			else {
				logger.error("Cannot fix desc renaming ( " + oldUi + " -> " + newUi + ") : " + oldUi + " disappeared in an unknown way.");
				return;
			}
		}
		
		// newUi ... already exists
		if (target.hasDescriptorByUi(newUi)) {
			logger.error("Cannot fix desc renaming ( " + oldUi + " -> " + newUi + ") : " + newUi + " already exists.");
			return;
		}
		
		descRen_target.put(oldUi,newUi);
	}
	
	
	private void fixDescRelabelling(String oldName, String newName) {
		// TODO later / don't need!?
		logger.warn("fixVertexRenamings() is not implemented! However, it is also not needed for creating the Semedico MeSH at the moment.");
	}
	
	
	private void fixVertexAddition(String vName, String parentName, String descUi) {
		// descriptor ...
		if (!target.hasDescriptorByUi(descUi) && !descAdd_source.containsByUI(descUi)) {

			// ... got renamed
			if(descRen_s2t.containsOld(descUi)) {
				descUi = descRen_s2t.getNew(descUi);			
				fixVertexAddition(vName, parentName, descUi);
				cntFix++;
				return;
			} 
			
			// ... got deleted
			else if (descDel_s2t.contains(descUi)){
				// what to do???
				logger.warn("Cannot fix vertex addition ( " + vName + " as parent of " + parentName + ") : desc with UI '" + descUi + "' got deleted.");
				return;
				
			}
			
			// is this else ok???? 2012/10/15
			else { 
				logger.error("Cannot fix vertex addition ( " + vName + " as parent of " + parentName + ") : descUi (" + descUi + ") disappeared in an unknown way.");
				return;
			}
		}
		
		// parent vertex
		if (!target.hasVertex(parentName) && !vertexAdd_source.contains(parentName)) {
			
			// ... got deleted
			if ( vertexDel_s2t.containsKey(parentName)) {
				// fix: add vertex as child of the parent of the deleted vertex
				parentName = source.parentVertexOf(parentName).getName();
			}
			
			// ... got moved
			else if ( vertexRe_s2t.containsOld(parentName)) {
				// fix: add vertex as child of moved vertex
				parentName = vertexRe_s2t.getNew(parentName);
			}
			
			// ... something else -> error
			else {
				logger.error("Cannot fix vertex addition ( " + vName + " as parent of " + parentName + ") : parent disappeared in an unknown way.");
				return;
			}
			
			fixVertexAddition(vName, parentName, descUi);
			cntFix++;
			return;
		}
		
		// vertex
		if (target.hasVertex(vName)) {
			logger.error("Cannot fix vertex addition ( " + vName + " as parent of " + parentName + ") : " + vName + " is already in Tree '" + source.getName() +"'.");
			return;
		}
		
		vertexAdd_target.put(vName, parentName, descUi);
	}
		
	
	private void fixVertexMoving(String vName, String oldParent,
			String newParent, String oldDescUi, String newDescUi) {
		// vertex ...
		if (!target.hasVertex(vName) && !vertexAdd_source.contains(vName) ) {

			// ... got deleted
			if ( vertexDel_s2t.containsKey(vName)) {
				
				// fix: move all children of vName in source instead
				for (TreeVertex child_source : source.childVerticesOf(source.getVertex(vName))) {
					fixVertexMoving(child_source.getName(), vName, newParent, child_source.getDescUi(), child_source.getDescUi());
				}
				cntFix++;
				return;
			}

			// ... got moved
			else if ( vertexRe_s2t.containsOld(vName)) {
				// fix: ... move the moved vertex anyway
				vName = vertexRe_s2t.getNew(vName);
				oldParent = target.parentVertexOf(vName).getName();
				oldDescUi = target.getVertex(vName).getDescUi();
				fixVertexMoving(vName, oldParent, newParent, oldDescUi, newDescUi);
				cntFix++;
				return;
			}

			// ... something else -> error
			else {
				logger.error("Cannot fix vertex moving 1 ( " + vName + " moves to be parent of " + newParent + ") : " + vName + " disappeared in an unknown way.");
				return;
			}
		}
		
		// new parent vertex
		if (!target.hasVertex(newParent) && !vertexAdd_source.contains(newParent)) {

			// ... got deleted
			if ( vertexDel_s2t.containsKey(newParent)) {
				// fix: move to parent of deleted vertex 
				newParent = source.parentVertexOf(newParent).getName();
			}

			// ... got moved
			else if ( vertexRe_s2t.containsOld(newParent)) {
				// fix: ... move the moved vertex anyway
				newParent = vertexRe_s2t.getNew(newParent);
				// oldParent = target.parentVertexOf(target.getVertex(newParent)).getName(); // this was just wrong
			}

			// ... something else -> error
			else {
				logger.error("Cannot fix vertex moving 2 ( " + vName + " moves to be parent of " + newParent + ") : " + newParent + " disappeared in an unknown way.");
				return;
			}
			
			fixVertexMoving(vName, oldParent, newParent, oldDescUi, newDescUi);
			cntFix++;
			return;
		}
		
		// new Descriptor UI ...
		if (!target.hasDescriptorByUi(newDescUi) && !descAdd_source.containsByUI(newDescUi)) {
			// ... got renamed
			if(descRen_s2t.containsOld(newDescUi)) {
				// fix : use that descriptor UI
				newDescUi = descRen_s2t.getNew(newDescUi);
			} 
			
			// ... got deleted
			else if (descDel_s2t.contains(newDescUi)){
				// fix : keep old descriptor
				newDescUi = oldDescUi;
				//logger.error("Cannot fix vertex moving ( " + vName + " moves as parent of " + newParent + ") : desc for newDescUI " + newDescUi + " got deleted - don't know what to do.");
				//return;
			}
			
			// ... something else -> error
			else {
				logger.error("Cannot fix vertex moving 3 ( " + vName + " moves to be parent of " + newParent + ") : desc for newDescUI " + newDescUi + " disappeared in an unknown way.");
				return;
			}
			
			fixVertexMoving(vName, oldParent, newParent, oldDescUi, newDescUi);
			cntFix++;
			return;
		}
		
		// old descriptor ...
		if (!target.hasDescriptorByUi(oldDescUi) && !descAdd_source.containsByUI(oldDescUi)) {
			// ... got renamed
			if(descRen_s2t.containsOld(oldDescUi)) {
				// fix : use that descriptor UI
				oldDescUi = descRen_s2t.getNew(oldDescUi);
			} 

			// ... got deleted
			else if (descDel_s2t.contains(oldDescUi)){
				// fix : take newDescUi as old. Reason: this is only of interest
				// if the vertex is rebound to a new descriptor. Then we need
				// the old descriptor UI to inform the descriptor that it
				// 'lost' one of its vertices. However, when the old descriptor
				// is deleted, there is no need for that anymore. Much more, it
				// is then suitable to set them equal, so that this is not
				// regarded a rebinding anymore.
				oldDescUi = newDescUi;
				//logger.error("Cannot fix vertex moving ( " + vName + " moves as parent of " + newParent + ") : desc for newDescUI " + newDescUi + " got deleted - don't know what to do.");
			}

			// ... something else -> error
			else {
				logger.error("Cannot fix vertex moving 4 ( " + vName + " moves to be parent of " + newParent + ") : desc for oldDescUI " + oldDescUi + " disappeared in an unknown way.");
				return;
			}

			fixVertexMoving(vName, oldParent, newParent, oldDescUi, newDescUi);
			cntFix++;
			return;
		}
		
		vertexMov_target.put(vName, oldParent, newParent, oldDescUi, newDescUi);
	}
	
	
	private void fixVertexDeletion(String vName, Boolean rec) {
		// vertex ...
		if (!target.hasVertex(vName) && !vertexAdd_source.contains(vName)) {

			// ... got deleted
			if ( vertexDel_s2t.containsKey(vName)) {
				
				if (!rec) {
					// fix: nothing to do - thanks for the work.
					return;
					
				} else {
					// fix: delete all children of vName in source instead
					for (TreeVertex child_source : source.childVerticesOf(source.getVertex(vName))) {
						fixVertexDeletion(child_source.getName(), rec);
					}
					cntFix++;
					return;				
				}
			}

			// ... got moved
			else if ( vertexRe_s2t.containsOld(vName)) {
				// fix: ... delete the moved vertex anyway
				vName = vertexRe_s2t.getNew(vName);
				fixVertexDeletion(vName, rec);
				cntFix++;
				return;
			}

			// ... something else -> error
			else {
				logger.error("Cannot fix vertex deletion of ( " + vName + ", rec = " + rec + " ) : it disappeared in an unknown way.");
				return;
			}
		}
		
		vertexDel_target.put(vName, rec);
	}


	private void fixDescDeletion(String descUi) {
		// desc ui ...
		if (!target.hasDescriptorByUi(descUi) && !descAdd_source.containsByUI(descUi)) {
			
			// ... got deleted
			if ( descDel_s2t.contains(descUi)) {
				// fix: no fix! thanks for the work!
				cntFix++;
			} 
			
			// ... got renamed
			else if ( descRen_s2t.containsOld(descUi)) {
				// fix : delete the renamed desc 
				fixDescDeletion (descRen_s2t.getNew(descUi));
				cntFix++;
			}
			
			// error!
			else {
				logger.error("Cannot fix desc deletion of " + descUi + " : it disappeared in an unknown way.");
			}
			return;
		}
		
		descDel_target.add(descUi);
	}
	
	
	private void fixVertexRenaming(String oldUi, String newUi) {
		// TODO later / don't need!?
		logger.warn("fixVertexRenamings() is not implemented! However, it is also not needed for creating the Semedico MeSH at the moment.");
	}

}
