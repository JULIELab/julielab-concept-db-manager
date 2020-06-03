package de.julielab.concepts.db.creators;

import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;

import java.util.HashSet;
import java.util.Set;

public class Node {
    private Set<Node> parents;
    private ImportConcept concept;
    private String id;


    public Node(String id) {
        this.id = id;
    }

    public void addParent(Node parent) {
        if (parents == null)
            parents = new HashSet<>();
        parents.add(parent);
    }

    public Set<Node> getParents() {
        return parents;
    }

    public void setParents(Set<Node> parents) {
        this.parents = parents;
    }

    public ImportConcept getConcept() {
        return concept;
    }

    public void setConcept(ImportConcept concept) {
        this.concept = concept;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
