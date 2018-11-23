package de.julielab.concepts.db.creators;

import com.google.common.collect.Sets;

import java.util.*;

public class GeneOrthologyCluster {
    private Map<String, Set<String>> geneid2cluster = new HashMap<>();
    private Map<String, Set<String>> geneid2mergedcluster = new HashMap<>();

    /**
     * This method is used to build the original gene_orthology information where one gene is associated with a set of other genes in the "Ortholog" relation.
     *
     * @param clusterId The source gene of the cluster.
     * @param geneId    A gene that is associated with the clusterId gene.
     */
    public void addToOriginalCluster(String clusterId, String geneId) {
        Set<String> cluster = geneid2cluster.get(clusterId);
        if (cluster == null) {
            cluster = new HashSet<>();
            cluster.add(clusterId);
            geneid2cluster.put(clusterId, cluster);
        }
        cluster.add(geneId);
    }

    public void mergeOverlappingClusters() {
        for (String geneid : geneid2cluster.keySet()) {
            final Set<String> cluster = geneid2cluster.get(geneid);
            Set<String> clusterunion = new HashSet<>(cluster);
            for (String gene : cluster) {
                final Set<String> othercluster = geneid2cluster.get(gene);
                clusterunion.addAll(othercluster);
            }
            geneid2mergedcluster.put(geneid, clusterunion);
        }
    }
}
