package de.julielab.concepts.db.creators;

import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;

import java.util.*;
import java.util.stream.Collectors;

public class Forest {
    private Map<String, Node> id2node = new HashMap<>();

    /**
     * Adds a new node with the given ID if there is not yet a node with that ID.
     *
     * @param id The node ID.
     * @return The node with the given ID, either the previously existing one or the newly created.
     */
    public Node addNode(String id) {
        Node n = id2node.get(id);
        if (n == null) {
            n = new Node(id);
            id2node.put(id, n);
        }
        return n;
    }

    public Node addNode(ImportConcept concept) {
        Set<String> parentIds = Collections.emptySet();
        if (concept.parentCoordinates != null)
            parentIds = concept.parentCoordinates.stream().map(cc -> cc.originalId).collect(Collectors.toSet());
        return addNode(concept.coordinates.originalId, parentIds);
    }

    public Node addNode(String id, Set<String> parentIds) {
        Node n = addNode(id);
        for (String pId : parentIds) {
            n.addParent(addNode(pId));
        }
        return n;
    }

    public Node addNode(String id, String parentId) {
        Node n = addNode(id);
        Node pn = addNode(parentId);
        n.addParent(pn);
        return n;
    }

    public Node addNode(String id, Node parent) {
        Node n = addNode(id);
        n.addParent(parent);
        return n;
    }

    public Set<Node> getRoots(String id) {
        Node n = id2node.get(id);
        if (n == null)
            return Collections.emptySet();
        if (n.getParents() == null || n.getParents().isEmpty())
            return Set.of(n);
        return n.getParents().stream().map(Node::getId).map(this::getRoots).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    public Optional<Node> getRoot(String geneId) {
        Set<Node> roots = getRoots(geneId);
        if (roots.size() > 1)
            throw new IllegalStateException("A single root for geneId " + geneId + " was expected but there were " + roots.size() + ".");
      return roots.stream().findAny();
    }
}
