package de.julielab.concepts.db.creators.mesh.modifications;

import java.util.*;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.TreeFilter;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * <p>
 * Class that deals with tree-vertex deletions. It's possible to add vertex
 * deletions, retrieve them, apply them to a <code>Tree</code> object and also
 * to summarize deletions.
 * </p>
 *
 * <p>
 * Essentially it is a map that maps tree-vertices which are to be deleted
 * to <code>VertexDeletions.RECURSIVE (== true)</code> for recursive deletion, and
 * to <code>VertexDeletions.SINGLE (== false)</code> for a single vertex deletion.
 * </p>
 *
 * <p>
 * As an additional functionality it allows to reason which descriptors have
 * been deleted from a <code>Tree</code> object. For that the <code>enableAdditonalDescInfos(boolean)</code>
 * <code>updateAdditonalDescriptorInfos(Tree data)</code>method is provided. However,
 * since this depends on a <code>Tree</code> object, which is not stored
 * internally, you need to update it manually, after changing the set of vertex
 * deletions!
 * </p>
 *
 * <p> Note:  Default behaviour is disabled additional descriptor information. </p>
 *
 * @author Philipp Lucas
 */

public class VertexDeletions
        extends LinkedHashMap<String, Boolean>
        implements TreeModficationsInterface {

    public static final boolean RECURSIVE = true;
    public static final boolean SINGLE = false;
    private static final long serialVersionUID = 5680653994566757185L;
    private static Logger logger = org.slf4j.LoggerFactory.getLogger(VertexDeletions.class);
    private boolean flagAdditionalDescInfo = false;

    // deleted vertices
    // this class extends HashMap -> that's where it is stored!

    // deleted descriptors
    // -> maps UIs descriptors to the set of the names of all those vertices of that descriptor that are deleted
    private LinkedHashMap<String, Set<String>> delDescs = new LinkedHashMap<String, Set<String>>();

    /**
     * Applies and thus changes the passed <code>Tree</code> object. Since
     * <code>apply</code> uses the <code>TreeFilter</code> class, the resulting
     * Tree will actually be a tree and not divided into several partitions. See
     * <code>TreeFilter</code> for details.
     *
     * @param data Tree object to apply deletions on.
     */
    public void apply(Tree data) {
        logger.info("# Deleting vertices from " + data.getName() + " ...");
        ProgressCounter counter = new ProgressCounter(size(), 10, "vertex");

        TreeFilter filter = new TreeFilter(data, TreeFilter.KEEP, TreeFilter.KEEP);
        for (String vertexName : keySet()) {
            if (!data.hasVertex(vertexName)) {
                logger.warn("Cannot delete vertex because vertex doesn't exist in my data. vertex-name was "
                        + vertexName);
                continue;
            }
            // TODO check boolean
            int recDep = (get(vertexName) ? -1 : 1);
            TreeVertex vertexToDel = data.getVertex(vertexName);
            filter.maskTreeVertex(vertexToDel, TreeFilter.THROW, recDep);
            counter.inc();
        }

        filter.apply();
        logger.info("# Done.");
    }


    /**
     * <p>
     * Detects recursive deletions (with respect to a Tree object) and
     * summarizes them as much as possible.
     * </p>
     * E.g. a vertex v and all its offspring will be deleted, then this deletion
     * operations can be summaries as a single recursive deletion of v.
     *
     * @param data A Tree object. Will not be modified.
     */
    public void detectRecursiveDeletions(Tree data) {
        traverseForRecDelDetection(data.getRootVertex(), data);
    }

    /**
     * Opposite of <code>detectRecursiveDeletions(Tree)</code>: it expands
     * recursive deletions of all tree vertices, such that there is an explicit
     * vertex deletion element for each vertex of <code>data</code> that would
     * be deleted when doing <code>apply(data)</code>.
     *
     * @param data A Tree object. Will not be modified.
     */
    public void expandRecursiveDeletions(Tree data) {
        traverseForRecDelExpansion(data.getRootVertex(), false, data);
    }


    /**
     * Recursive method to do what <code>expandRecursiveDeletions</code> is called to do...
     *
     * @param v      Current tree vertex.
     * @param recDel Flag. Is <code>true</code> if <code>v</code> is marked to be deleted recursively.
     * @param data   <code>Tree</code> instance to operate on.
     */
    private void traverseForRecDelExpansion(TreeVertex v, boolean recDel, Tree data) {
        for (TreeVertex child : data.childVerticesOf(v)) {
            // if either an ancestor is deleted recursively or child itself ...
            if (recDel || (containsKey(child.getName()) && get(child.getName()))) {
                // ... then add this as explicit deletion
                super.put(child.getName(), true);
                // ... and also delete all offspring!
                traverseForRecDelExpansion(child, true, data);
            }
            // otherwise just continue to traverse
            else {
                traverseForRecDelExpansion(child, false, data);
            }
        }
    }


    /**
     * This method analyses for tree vertex <code>v</code> and all its offspring
     * which non-recursive deletions (of them) can be wrapped together into
     * recursive deletions.
     *
     * @param v A tree vertex
     * @return Returns true if <code>v</code> and all its offspring is deleted.
     */
    private boolean traverseForRecDelDetection(TreeVertex v, Tree data) {
        boolean isAllDeleted = true;

        // check if children of v are fully deleted
        Set<TreeVertex> fullyDeleted = new LinkedHashSet<TreeVertex>();
        for (TreeVertex child : data.childVerticesOf(v)) {
            boolean res = traverseForRecDelDetection(child, data);
            if (res) {
                fullyDeleted.add(child);
            }
            isAllDeleted = isAllDeleted && res;
        }

        if (isAllDeleted & containsKey(v.getName())) {
            return true;
        } else {
            for (TreeVertex del : fullyDeleted) {
                traverseForRecDelMarking(del, data);
                super.put(del.getName(), true);
            }
            return false;
        }
    }

    /**
     * Helper method for <code>traverseForRecDelDetection</code>. It will remove
     * <code>v</code> and all its offspring from <code>vertexDeletions</code>.
     *
     * @param v A tree vertex.
     */
    private void traverseForRecDelMarking(TreeVertex v, Tree data) {
        remove(v.getName());
        for (TreeVertex child : data.childVerticesOf(v)) {
            traverseForRecDelMarking(child, data);
        }
    }

    /**
     * Enables or disables additional descriptor information output in files
     * when exporting via <code>ModificationExporter.saveVertexDeletions</code>.
     *
     * @param flag true for enable, false for disable.
     */
    public void enableAdditonalDescInfos(boolean flag) {
        flagAdditionalDescInfo = flag;
    }

    /**
     * @return Returns if additional descriptor information will
     */
    public boolean isAdditonalDescInfos() {
        return flagAdditionalDescInfo;
    }

    /**
     * Updates a set of information with respect to <code>data</code>:
     * <ol>
     *  <li>vertex deletions per descriptor: For each descriptor that loses at least one vertex, the vertex names of all its vertex deletions.</li>
     * </ol>
     *
     * @param data Is the <code>Tree</code> object which is subject to the deletions.
     */
    public void updateAdditonalDescriptorInfos(Tree data) {
        // reset
        delDescs.clear();

        // deleted vertices per descriptor
        for (String vertexName : this.keySet()) {
            TreeVertex v = data.getVertex(vertexName);
            Set<String> names;
            if (delDescs.containsKey(v.getDescUi())) {
                names = delDescs.get(v.getDescUi());
            } else {
                names = new LinkedHashSet<String>();
            }
            names.add(v.getName());
            delDescs.put(v.getDescUi(), names);
        }

    }

    /**
     * @return Returns a map containing for each Descriptor UI the set of those
     * vertex names whose corresponding vertices have been deleted.
     */
    public Map<String, Set<String>> getDelDescs() {
        return delDescs;
    }

    /**
     * Inserts <code>descUi</code> to this set, if not already there.
     * Returns true iff successfully inserted, false otherwise.
     */
    @Override
    public Boolean put(String vertexName, Boolean rec) {
        if (containsKey(vertexName)) {
            logger.warn("Overwriting existing descriptor deletion : " + toString(vertexName));
        }
        return super.put(vertexName, rec);
    }


    public String toString(String vertexName) {
        if (containsKey(vertexName)) {
            return "vertex : " + vertexName + " --- recursive : " + get(vertexName);
        } else {
            return "";
        }
    }

    @Override
    public String toString() {
        StringBuffer out = new StringBuffer();
        for (String v : keySet()) {
            out.append(toString(v) + "\n");
        }
        return out.toString();
    }


    @Override
    public boolean isEmpty() {
        return super.isEmpty();
    }

}
