package de.julielab.concepts.db.creators;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import de.julielab.concepts.db.core.DefaultFacetCreator;
import de.julielab.concepts.db.core.services.FacetCreationService;
import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.semedico.resources.ResourceTermLabels;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class NCBIGeneConceptCreator implements ConceptCreator {

    public static final String SEMEDICO_RESOURCE_MANAGEMENT_SOURCE = "Semedico Resource Management";
    public static final String NCBI_GENE_SOURCE = "NCBI Gene";
    public static final String BASEPATH = "basepath";
    public static final String GENE_INFO = "gene_info";
    public static final String GENEDESCRIPTIONS = "genedescriptions";
    public static final String ORGANISMLIST = "organismlist";
    public static final String ORGANISMNAMES = "organismnames";
    public static final String HOMOLOGENE = "homologene";
    public static final String GENE_GROUP = "gene_group";
    public static final String HOMOLOGENE_PREFIX = "homologene";
    /**
     * "gene_group" is the name of the file specifying the ortholog relationships
     * between genes. Also, NCBI Gene, searching for a specific ortholog group works
     * by search for "ortholog_gene_2475[group]" where the number is the ID of the
     * gene that represents the group, the human gene, most of the time.
     */
    public static final String GENE_GROUP_PREFIX = "genegroup";
    public static final String TOP_ORTHOLOGY_PREFIX = "toporthology";
    public static final String TOP_HOMOLOGY_PREFIX = "tophomology";
    private int homologeneAggregateCounter;
    private int orthologAggregateCounter;
    private int topOrthologAggregateCounter;
    private int topHomologyAggregateCounter;
    private Logger log = LoggerFactory.getLogger(NCBIGeneConceptCreator.class);

    public NCBIGeneConceptCreator() {
        resetCounters();
    }

    private void resetCounters() {
        this.homologeneAggregateCounter = 0;
        this.orthologAggregateCounter = 0;
        this.topOrthologAggregateCounter = 0;
        this.topHomologyAggregateCounter = 0;
    }

    /**
     * @param termsByGeneId
     * @param homologene
     * @param geneGroup     see https://ncbiinsights.ncbi.nlm.nih.gov/2018/02/27/gene_orthologs-file-gene-ftp/
     * @throws IOException
     */
    private void createHomologyAggregates(Map<ConceptCoordinates, ImportConcept> termsByGeneId, File homologene,
                                          File geneGroup) throws IOException {
        Multimap<String, ConceptCoordinates> genes2Aggregate = HashMultimap.create();

        List<String> aggregateCopyProperties = Arrays.asList(ConceptConstants.PROP_PREF_NAME,
                ConceptConstants.PROP_SYNONYMS, ConceptConstants.PROP_WRITING_VARIANTS,
                ConceptConstants.PROP_DESCRIPTIONS, ConceptConstants.PROP_FACETS);

        createHomologeneAggregates(genes2Aggregate, homologene, termsByGeneId, aggregateCopyProperties);
        createGeneOrthologyAggregates(genes2Aggregate, geneGroup, termsByGeneId, aggregateCopyProperties);
        createTopHomologyAggregates(genes2Aggregate, termsByGeneId, aggregateCopyProperties);


    }

    private void createTopHomologyAggregates(Multimap<String, ConceptCoordinates> genes2Aggregate, Map<ConceptCoordinates, ImportConcept> termsByGeneId, List<String> aggregateCopyProperties) {
        // Now create top homology aggregates where necessary and connect the
        // top homology aggregate to the gene group and homology aggregates.
        for (String geneId : genes2Aggregate.keySet()) {
            final ImportConcept gene = termsByGeneId.get(getGeneCoordinates(geneId));
            ImportConcept topHomologyAggregate = findTopHomologyAggregate(gene, termsByGeneId);
            if (topHomologyAggregate == null) {
                Set<ImportConcept> topAggregates = findTopOrthologsAndHomologyAggregates(gene, termsByGeneId, new TreeSet<>(Comparator.comparingLong(System::identityHashCode)));
                // Only if there is more then one aggregate for the current gene we need a new top aggregate to unite the existing aggregates
                if (topAggregates.size() > 1) {
                    topHomologyAggregate = new ImportConcept(topAggregates.stream().map(ic -> ic.coordinates).collect(toList()), aggregateCopyProperties);
                    topHomologyAggregate.coordinates = new ConceptCoordinates();
                    topHomologyAggregate.coordinates.sourceId = TOP_HOMOLOGY_PREFIX + topHomologyAggregateCounter;
                    topHomologyAggregate.coordinates.source = SEMEDICO_RESOURCE_MANAGEMENT_SOURCE;
                    topHomologyAggregate.aggregateIncludeInHierarchy = true;
                    topHomologyAggregate.generalLabels = Arrays.asList("AGGREGATE_TOP_HOMOLOGY",
                            "NO_PROCESSING_GAZETTEER");
                    ConceptCoordinates topHomologyCoordinates = topHomologyAggregate.coordinates;
                    termsByGeneId.put(topHomologyAggregate.coordinates, topHomologyAggregate);
                    topAggregates.forEach(agg -> agg.addParent(topHomologyCoordinates));
                    ++topHomologyAggregateCounter;
                }
            }
        }
    }

    private Set<ImportConcept> findTopOrthologsAndHomologyAggregates(ImportConcept concept, Map<ConceptCoordinates, ImportConcept> termsByGeneId, Set<ImportConcept> importConcepts) {
        // Finds aggregates that are a gene orthology aggregate without a top orthology aggregate, are a top orthology aggregate or a homology aggregate
        Predicate<ImportConcept> isSought = c -> (c.coordinates.sourceId.startsWith(GENE_GROUP_PREFIX) && !c.parentCoordinates.stream().map(termsByGeneId::get).filter(p -> p.coordinates.sourceId.startsWith(TOP_ORTHOLOGY_PREFIX)).findAny().isPresent()) || c.coordinates.sourceId.startsWith(TOP_ORTHOLOGY_PREFIX) || c.coordinates.sourceId.startsWith(HOMOLOGENE_PREFIX);
        if (isSought.test(concept))
            importConcepts.add(concept);
        for (ImportConcept parent : (Iterable<ImportConcept>) () -> concept.parentCoordinates.stream().map(termsByGeneId::get).iterator()) {
            findTopOrthologsAndHomologyAggregates(parent, termsByGeneId, importConcepts);
        }
        return importConcepts;
    }

    private ImportConcept findTopHomologyAggregate(ImportConcept concept, Map<ConceptCoordinates, ImportConcept> termsByGeneId) {
        if (concept.coordinates.sourceId.startsWith(TOP_HOMOLOGY_PREFIX))
            return concept;
        ImportConcept topHomologyConcept = null;
        if (concept.parentCoordinates == null)
            throw new IllegalArgumentException("The passed concept does have null parents " + concept.coordinates);
        for (ConceptCoordinates parentCoordinates : concept.parentCoordinates) {
            final ImportConcept parent = termsByGeneId.get(parentCoordinates);
            topHomologyConcept = findTopHomologyAggregate(parent, termsByGeneId);
            if (topHomologyConcept != null)
                break;
        }
        return topHomologyConcept;
    }


    private void createGeneOrthologyAggregates(Multimap<String, ConceptCoordinates> genes2Aggregate, File geneGroup, Map<ConceptCoordinates, ImportConcept> termsByGeneId, List<String> aggregateCopyProperties) throws IOException {
        // add the orthology information from gene group to make gene group aggregates
        Map<String, Set<String>> geneGroupOrthologs = new HashMap<>();
        final Iterator<String> iterator = FileUtilities.getReaderFromFile(geneGroup).lines().iterator();
        // Format: tax_id GeneID relationship Other_tax_id Other_GeneID (tab is
        // used as a separator, pound sign - start of a comment)
        while (iterator.hasNext()) {
            String geneGroupLine = iterator.next();
            if (geneGroupLine.startsWith("#"))
                continue;
            String[] geneGroupRecord = geneGroupLine.split("\t");
            if (geneGroupRecord.length < 5)
                throw new IllegalArgumentException("The line " + geneGroupLine + " does not have at least 5 tab-separated columns.");
            String relationship = geneGroupRecord[2];
            if (!relationship.equals("Ortholog"))
                continue;
            String gene1 = geneGroupRecord[1];
            String gene2 = geneGroupRecord[4];
            geneGroupOrthologs.compute(gene1, (gene, set) -> {
                Set<String> newset = set;
                if (newset == null) newset = new HashSet<>();
                newset.add(gene2);
                return newset;
            });
        }
        log.info("Got {} orthology groups from gene_orthology file {}", geneGroupOrthologs.size(), geneGroup);

        // 1. create separate gene orthology aggregates
        // 2. for overlapping gene orthology aggregates, create a top-orthology aggregate
        // 3. when there is a non-empty intersection between homologene and (top) gene
        // orthology aggregate elements, create a top homology aggregate
        // 4. set the new top homology aggregate as parent of the homologene and
        // (top) group aggregate nodes
        Multimap<String, ImportConcept> genes2OrthoAggregate = HashMultimap.create();
        for (String geneGroupId : geneGroupOrthologs.keySet()) {
            Collection<String> mappingTargets = geneGroupOrthologs.get(geneGroupId);
            List<String> groupGeneIds = new ArrayList<>(mappingTargets.size() + 1);
            List<ConceptCoordinates> groupGeneCoords = new ArrayList<>(mappingTargets.size() + 1);
            // Create coordinates for this gene cluster's genes
            for (String geneId : mappingTargets) {
                // it is possible that some elements of a gene group are not in
                // our version of gene_info (e.g. due to species filtering)
                ConceptCoordinates geneCoords = getGeneCoordinates(geneId);
                if (!termsByGeneId.containsKey(geneCoords)) {
                    continue;
                }
                groupGeneIds.add(geneId);
                groupGeneCoords.add(geneCoords);
            }
            // The gene group ID is also a valid gene. Most of the time the
            // human version. It has to be added to the resulting aggregate
            // node as well.
            // But here also we should check if we even know a gene with this ID
            if (termsByGeneId.containsKey(getGeneCoordinates(geneGroupId))) {
                groupGeneIds.add(geneGroupId);
                groupGeneCoords.add(getGeneCoordinates(geneGroupId));
            }

            // Create the aggregate for this orthology gene cluster.
            // The set of genes participating in this gene group might be empty or only
            // contain a single element because all other elements were not included in the
            // input gene_info. Then, we don't need an aggregate.
            if (groupGeneCoords.size() > 1) {
                ImportConcept orthologyCluster = new ImportConcept(groupGeneCoords, aggregateCopyProperties);
                orthologyCluster.coordinates = new ConceptCoordinates();
                orthologyCluster.coordinates.sourceId = GENE_GROUP_PREFIX + geneGroupId;
                orthologyCluster.coordinates.source = "GeneOrthology";
                orthologyCluster.coordinates.originalSource = "GeneOrthology";
                orthologyCluster.coordinates.originalId = geneGroupId;
                orthologyCluster.aggregateIncludeInHierarchy = true;
                orthologyCluster.generalLabels = Arrays.asList("AGGREGATE_GENEGROUP", "NO_PROCESSING_GAZETTEER");
                termsByGeneId.put(orthologyCluster.coordinates,
                        orthologyCluster);
                ++orthologAggregateCounter;

                for (String geneId : groupGeneIds) {
                    ImportConcept gene = termsByGeneId.get(getGeneCoordinates(geneId));
                    genes2OrthoAggregate.put(geneId, orthologyCluster);
                    genes2Aggregate.put(geneId,
                            new ConceptCoordinates(orthologyCluster.coordinates.sourceId, orthologyCluster.coordinates.source, true));
                    gene.addParent(orthologyCluster.coordinates);
                    // If we actually aggregate multiple genes into one, the
                    // elements should disappear behind the aggregate and as such
                    // should not be present in the query dictionary or suggestions.

                    if (groupGeneIds.size() > 1) {
                        gene.addGeneralLabel(ResourceTermLabels.Gazetteer.NO_QUERY_DICTIONARY.name(),
                                ResourceTermLabels.Suggestions.NO_SUGGESTIONS.name());
                    }
                }
            }
        }

        // Create top-orthology aggregates for genes taking part in multiple orthology clusters
        Map<ConceptCoordinates, ImportConcept> orthoAgg2TopOrtho = new HashMap<>();
        for (String geneid : genes2OrthoAggregate.keySet()) {
            final Collection<ImportConcept> clusters = genes2OrthoAggregate.get(geneid);
            // If there is only one cluster associated with the current gene, we don't need to do anything here
            if (clusters.size() > 1) {
                ImportConcept topOrthologyAggregate = null;
                // Find an already existing top orthology cluster, if existing
                Set<ImportConcept> seenOrthologyClusters = new TreeSet<>(Comparator.comparingLong(System::identityHashCode));
                for (ImportConcept cluster : clusters) {
                    topOrthologyAggregate = findTopOrtholog(cluster, seenOrthologyClusters, genes2OrthoAggregate, orthoAgg2TopOrtho);
                    if (topOrthologyAggregate != null)
                        break;
                }
                // If there is not yet a top orthology aggregate, create it now
                if (topOrthologyAggregate == null) {
                    topOrthologyAggregate = new ImportConcept(new ArrayList<>(), aggregateCopyProperties);
                    topOrthologyAggregate.coordinates = new ConceptCoordinates();
                    topOrthologyAggregate.coordinates.sourceId = TOP_ORTHOLOGY_PREFIX + topOrthologAggregateCounter;
                    topOrthologyAggregate.coordinates.source = SEMEDICO_RESOURCE_MANAGEMENT_SOURCE;
                    topOrthologyAggregate.aggregateIncludeInHierarchy = true;
                    topOrthologyAggregate.generalLabels = Arrays.asList("AGGREGATE_TOP_ORTHOLOGY", "NO_PROCESSING_GAZETTEER");
                    termsByGeneId.put(topOrthologyAggregate.coordinates,
                            topOrthologyAggregate);
                    ++topOrthologAggregateCounter;
                }
                // Connect the current gene orthology clusters to the top orthology aggregate
                for (ImportConcept cluster : clusters) {
                    ConceptCoordinates clusterCoordinates = cluster.coordinates;
                    if (!topOrthologyAggregate.elementCoordinates.contains(clusterCoordinates))
                        topOrthologyAggregate.elementCoordinates.add(clusterCoordinates);
                    orthoAgg2TopOrtho.put(clusterCoordinates, topOrthologyAggregate);
                    cluster.addParent(topOrthologyAggregate.coordinates);
                }
            }
        }
    }

    private ImportConcept findTopOrtholog(ImportConcept orthologyCluster, Set<ImportConcept> seenOrthologyClusters, Multimap<String, ImportConcept> genes2OrthoAggregate, Map<ConceptCoordinates, ImportConcept> orthoAgg2TopOrtho) {
        // First check if the orthology cluster is already connected to a top orthology aggregate
        ImportConcept topOrtholog = orthoAgg2TopOrtho.get(orthologyCluster.coordinates);
        seenOrthologyClusters.add(orthologyCluster);
        if (topOrtholog == null) {
            // Try to find a top orthology aggregate that is connected - perhaps indirectly - through one of the orthology cluster's gene elements
            for (ConceptCoordinates element : orthologyCluster.elementCoordinates) {
                final Collection<ImportConcept> orthologyClustersOfElement = genes2OrthoAggregate.get(element.originalId);
                for (ImportConcept orthologyClusterOfElement : orthologyClustersOfElement) {
                    // Do not make a recursive call to the original orthology cluster because then we would never terminate
                    if (seenOrthologyClusters.contains(orthologyClusterOfElement))
                        continue;
                    seenOrthologyClusters.add(orthologyClusterOfElement);
                    // Make a recursive call to all of the other orthology clusters of the current gene element.
                    // Through the recursion we will follow the connections from gene elements to their orthology clusters, to their elements until there is nothing more to go to.
                    // In order to not go back the cluster where the search started, we pass the current gene's element id
                    topOrtholog = findTopOrtholog(orthologyClusterOfElement, seenOrthologyClusters, genes2OrthoAggregate, orthoAgg2TopOrtho);
                    if (topOrtholog != null)
                        return topOrtholog;
                }
            }
        }
        return topOrtholog;
    }

    private void createHomologeneAggregates(Multimap<String, ConceptCoordinates> genes2Aggregate, File homologene, Map<ConceptCoordinates, ImportConcept> termsByGeneId, List<String> aggregateCopyProperties) throws IOException {
        final Stream<HomologeneRecord> recordStream = FileUtilities.getReaderFromFile(homologene).lines().map(line -> line.split("\t")).map(HomologeneRecord::new);
        Multimap<String, HomologeneRecord> groupId2Homolo = Multimaps.index(recordStream.iterator(), r -> r.groupId);
        log.info("Got {} homologene records", groupId2Homolo.size());
        Map<String, String> genes2HomoloGroup = new HashMap<>();
        for (String groupId : groupId2Homolo.keySet()) {
            Collection<HomologeneRecord> group = groupId2Homolo.get(groupId);
            List<String> homologuousGeneIds = new ArrayList<>(group.size());
            List<String> homologuousGeneSources = new ArrayList<>(group.size());
            List<ConceptCoordinates> homologuousGeneCoords = new ArrayList<>(group.size());
            for (HomologeneRecord record : group) {
                String geneId = record.geneId;
                ConceptCoordinates geneCoords = getGeneCoordinates(geneId);
                if (!termsByGeneId.containsKey(geneCoords))
                    continue;
                homologuousGeneIds.add(geneId);
                homologuousGeneSources.add(NCBI_GENE_SOURCE);
                homologuousGeneCoords.add(geneCoords);
            }
            // Only continue if more than one gene in this homologene cluster are included
            // in our gene_info
            if (homologuousGeneCoords.size() >= 1) {
                ImportConcept aggregate = new ImportConcept(homologuousGeneCoords, aggregateCopyProperties);
                aggregate.coordinates = new ConceptCoordinates();
                aggregate.coordinates.sourceId = HOMOLOGENE_PREFIX + groupId;
                aggregate.coordinates.source = "Homologene";
                aggregate.coordinates.originalSource = "Homologene";
                aggregate.coordinates.originalId = groupId;
                aggregate.aggregateIncludeInHierarchy = true;
                aggregate.generalLabels = Arrays.asList("AGGREGATE_HOMOLOGENE", "NO_PROCESSING_GAZETTEER");
                termsByGeneId.put(aggregate.coordinates,
                        aggregate);
                ++homologeneAggregateCounter;
            if (aggregate.parentCoordinates == null)
                throw new IllegalStateException("WTF. " + aggregate);
                for (ConceptCoordinates geneCoords : homologuousGeneCoords) {
                    String geneId = geneCoords.originalId;
                    ImportConcept gene = termsByGeneId.get(geneCoords);
                    if (genes2HomoloGroup.containsKey(geneId))
                        throw new IllegalStateException(
                                "Gene with ID " + geneId + " is taking part in multiple homologene groups.");
                    genes2HomoloGroup.put(geneId, groupId);
                    genes2Aggregate.put(geneId,
                            new ConceptCoordinates(aggregate.coordinates.sourceId, aggregate.coordinates.source, true));
                    gene.addParent(aggregate.coordinates);
                    // If we actually aggregate multiple genes into one, the
                    // elements should disappear behind the aggregate and as such
                    // should not be present in the query dictionary or suggestions.

                    if (homologuousGeneIds.size() > 1) {
                        gene.addGeneralLabel(ResourceTermLabels.Gazetteer.NO_QUERY_DICTIONARY.name(),
                                ResourceTermLabels.Suggestions.NO_SUGGESTIONS.name());
                    }
                }
            }
        }
    }

    private List<ImportConcept> makeTermList(Map<ConceptCoordinates, ImportConcept> termsByGeneId) {
        List<ImportConcept> terms = new ArrayList<>(termsByGeneId.size());
        for (ImportConcept term : termsByGeneId.values()) {
            terms.add(term);
        }
        return terms;
    }

    /**
     * Gives genes species-related qualifier / display name in the form the NCBI
     * gene search engine does, e.g. interleukin 2 [Homo sapiens (human)], only that
     * we don't use the full official symbol but just the symbol to keep it a bit
     * shorter.
     *
     * @param ncbiTaxNames
     * @param geneId2Tax
     * @param geneTerms
     * @throws IOException
     */
    private void setSpeciesQualifier(File ncbiTaxNames, Map<String, String> geneId2Tax,
                                     Collection<ImportConcept> geneTerms) throws IOException {
        Map<String, TaxonomyRecord> taxNameRecords = new HashMap<>();
        Iterator<String> lineIt = FileUtilities.getReaderFromFile(ncbiTaxNames).lines().iterator();
        while (lineIt.hasNext()) {
            String recordString = lineIt.next();
            // at the end of the line there is no more tab, thus we have
            // actually
            // two record seperators
            String[] split = recordString.split("(\t\\|\t)|(\t\\|)");
            String taxId = split[0];
            String name = split[1];
            String nameClass = split[3];

            TaxonomyRecord record = taxNameRecords.get(taxId);
            if (null == record) {
                record = new TaxonomyRecord(taxId);
                taxNameRecords.put(taxId, record);
            }
            if (nameClass.equals("scientific name"))
                record.scientificName = name;
            else if (nameClass.equals("genbank common name"))
                record.geneBankCommonName = name;
        }

        for (ImportConcept gene : geneTerms) {
            String taxId = geneId2Tax.get(gene.coordinates.originalId);
            TaxonomyRecord taxonomyRecord = taxNameRecords.get(taxId);

            if (null == taxonomyRecord)
                throw new IllegalStateException("No NCBI Taxonomy name record was found for the taxonomy ID " + taxId);

            // Set the species as a qualifier
            String speciesQualifier = taxonomyRecord.scientificName;
            if (null != taxonomyRecord.geneBankCommonName)
                speciesQualifier += " (" + taxonomyRecord.geneBankCommonName + ")";
            gene.addQualifier(speciesQualifier);

            // Set an NCBI Gene like species-related display name.
            gene.displayName = gene.prefName + " [" + taxonomyRecord.scientificName;
            if (null != taxonomyRecord.geneBankCommonName)
                gene.displayName += " (" + taxonomyRecord.geneBankCommonName + ")";
            gene.displayName += "]";
        }
    }

    protected void convertGeneInfoToTerms(File geneInfo, File organisms, File geneDescriptions,
                                          Map<String, String> geneId2Tax, Map<ConceptCoordinates, ImportConcept> termsByGeneId) throws IOException {
        Set<String> organismSet = FileUtilities.getReaderFromFile(organisms).lines().collect(Collectors.toSet());

        Iterator<String> lineIt = FileUtilities.getReaderFromFile(geneDescriptions).lines().iterator();
        Map<String, String> gene2Summary = new HashMap<>();
        while (lineIt.hasNext()) {
            String line = lineIt.next();
            String[] split = line.split("\t");
            String geneId = split[0];
            String summary = split[1];
            gene2Summary.put(geneId, summary);
        }

        try (BufferedReader bw = FileUtilities.getReaderFromFile(geneInfo)) {
            Iterator<String> it = bw.lines().filter(record -> !record.startsWith("#")).iterator();
            while (it.hasNext()) {
                String record = it.next();
                ImportConcept term = createGeneTerm(record, gene2Summary);
                String[] split = record.split("\t", 2);
                String taxId = split[0];
                if (organismSet.contains(taxId) || organismSet.isEmpty()) {
                    geneId2Tax.put(term.coordinates.originalId, taxId);
                    termsByGeneId.put(term.coordinates,
                            term);
                }
            }
        }

    }

    private ImportConcept createGeneTerm(String record, Map<String, String> gene2Summary) {
        // 0: tax_id
        // 1: GeneID
        // 2: Symbol
        // 3: LocusTag
        // 4: Synonyms
        // 5: dbXrefs
        // 6: chromosome
        // 7: map_location
        // 8: description
        // 9: type_of_gene
        // 10: Symbol_from_nomenclature_authority
        // 11: Full_name_from_nomenclature_authority
        // 12: Nomenclature_status
        // 13: Other_designations
        // 14: Modification_date
        String[] split = record.split("\t");
        List<String> synonyms = new ArrayList<>();
        String prefName = split[2];
        String fullname = split[11];
        // It happens that genes have the official symbol 'e' or 'C' or 'N'; but
        // it seems those are kind of errorneous.
        // It's about 70 cases so not a big deal. We just use the full name,
        // then, and forget about the one-character
        // symbol.
        if (prefName.length() < 3 && fullname.length() > 2) {
            prefName = fullname;
        } else {
            synonyms.add(fullname);
        }
        String ncbiDescription = split[8];
        if (prefName.length() < 3 && ncbiDescription.length() > 2)
            prefName = ncbiDescription;
        String originalId = split[1];
        String synonymString = split[4];
        String otherDesignations = split[13];
        // synonyms:
        // 1. official full name (if not used as preferred name)
        // 2. synonyms
        // 3. other designations
        String[] synonymSplit = synonymString.split("\\|");
        for (int i = 0; i < synonymSplit.length; i++) {
            String synonym = synonymSplit[i];
            synonyms.add(synonym);
        }
        String[] otherDesignationsSplit = otherDesignations.split("\\|");
        for (int i = 0; i < otherDesignationsSplit.length; i++) {
            String synonym = otherDesignationsSplit[i];
            synonyms.add(synonym);
        }
        String description = gene2Summary.get(originalId);

        // remove synonyms that are too short
        for (Iterator<String> synonymIt = synonyms.iterator(); synonymIt.hasNext(); ) {
            if (synonymIt.next().length() < 2)
                synonymIt.remove();
        }
        ImportConcept geneTerm = new ImportConcept(prefName, synonyms, description,
                getGeneCoordinates(originalId));

        /**
         * Gene IDs are given by a Gene Normalization component like GeNo. Thus, genes
         * are not supposed to be additionally tagged by a gazetteer.
         */
        geneTerm.addGeneralLabel(ResourceTermLabels.Gazetteer.NO_PROCESSING_GAZETTEER.toString(),
                ResourceTermLabels.IdMapping.ID_MAP_NCBI_GENES.toString());

        return geneTerm;

    }

    public static ConceptCoordinates getGeneCoordinates(String originalId) {
        return new ConceptCoordinates(originalId, NCBI_GENE_SOURCE, originalId, NCBI_GENE_SOURCE);
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        String base = slash(basePath, CONCEPTS, CREATOR, CONFIGURATION);
        template.addProperty(slash(basePath, CONCEPTS, CREATOR, NAME), getName());
        template.addProperty(slash(base, BASEPATH), "");
        template.addProperty(slash(base, GENE_INFO), "");
        template.addProperty(slash(base, GENEDESCRIPTIONS), "");
        template.addProperty(slash(base, ORGANISMLIST), "");
        template.addProperty(slash(base, ORGANISMNAMES), "");
        template.addProperty(slash(base, HOMOLOGENE), "");
        template.addProperty(slash(base, GENE_GROUP), "");
        FacetCreationService.getInstance().exposeParameters(basePath, template);
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, FACET_GROUP, NAME), "Biology");
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, NAME), "Genes");
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, DefaultFacetCreator.SOURCE_TYPE), FacetConstants.SRC_TYPE_HIERARCHICAL);
    }

    /**
     * <ul>
     * <li>geneInfo
     * Original gene_info file download from the NCBI. Should reside on
     * our servers at
     * <tt>/data/data_resources/biology/entrez/gene/gene_info</tt> (or
     * similar, path could change over time).
     * </li>
     * <li>organisms
     * A list of NCBI Taxonomy IDs specifying the organisms for which
     * genes should be included. The whole of the gene database contains
     * around 16M entries, as of August 2014, most of which do not stand
     * in the focus of research. The list given here should be the same
     * list used for GeNo resource generation (organisms.taxid) to create
     * a match between terms in the term database and actually mapped
     * genes in the documents.</li>
     * <li>ncbiTaxNames
     * The <tt>names.dmp</tt> file included in the original NCBI Taxonomy
     * download. Should reside on our servers at
     * <tt>/data/data_resources/biology/ncbi_tax/names.dmp</tt> (or
     * similar, path could change over time).</li>
     * <li>geneSummary
     * This file - unfortunately - cannot be downloaded directly.
     * However, it should already exist, somewhere, since it is part of
     * GeNo resource generation. You can either ask someone who is
     * responsible for GeNo, or just build the semantic context index
     * yourself with the script that is included in the
     * jules-gene-mapper-ae project. Please note that summary download
     * takes a while (a few hours) and thus is filtered to only download
     * summaries for the genes that are included in GeNo.</li>
     * <li>homologene</li>
     * </ul>
     *
     * @throws FacetCreationException
     * @throws IOException
     */
    @Override
    public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig)
            throws ConceptCreationException, FacetCreationException {
        resetCounters();
        String confPath = slash(CONCEPTS, CREATOR, CONFIGURATION);
        try {
            ConfigurationUtilities.checkParameters(importConfig, slash(confPath, GENE_INFO), slash(confPath, GENEDESCRIPTIONS), slash(confPath, ORGANISMLIST),
                    slash(confPath, ORGANISMNAMES), slash(confPath, HOMOLOGENE), slash(confPath, GENE_GROUP));
        } catch (ConfigurationException e) {
            throw new ConceptCreationException(e);
        }

        String basepath = importConfig.getString(slash(confPath, BASEPATH), "");
        File geneInfo = resolvePath(basepath, importConfig.getString(slash(confPath, GENE_INFO)));
        File geneDescriptions = resolvePath(basepath, importConfig.getString(slash(confPath, GENEDESCRIPTIONS)));
        File organisms = resolvePath(basepath, importConfig.getString(slash(confPath, ORGANISMLIST)));
        File ncbiTaxNames = resolvePath(basepath, importConfig.getString(slash(confPath, ORGANISMNAMES)));
        File homologene = resolvePath(basepath, importConfig.getString(slash(confPath, HOMOLOGENE)));
        File geneGroup = resolvePath(basepath, importConfig.getString(slash(confPath, GENE_GROUP)));
        List<File> notFound = new ArrayList<>();
        for (File f : Arrays.asList(geneInfo, geneDescriptions, organisms, ncbiTaxNames, homologene, geneGroup)) {
            if (!f.exists())
                notFound.add(f);
        }
        if (!notFound.isEmpty())
            throw new ConceptCreationException("The following files do not exist: " + notFound.stream()
                    .map(File::getAbsolutePath).collect(joining(System.getProperty("line.separator"))));

        try {
            log.info("Beginning import of NCBI Genes.");
            Map<String, String> geneId2Tax = new HashMap<>();
            Map<ConceptCoordinates, ImportConcept> termsByGeneId = new HashMap<>();
            log.info("Converting NCBI Gene source files into Semedico terms.");
            convertGeneInfoToTerms(geneInfo, organisms, geneDescriptions, geneId2Tax, termsByGeneId);
            setSpeciesQualifier(ncbiTaxNames, geneId2Tax, termsByGeneId.values());
            log.info("Got {} terms from source files..", termsByGeneId.values().size());
            log.info("Creating homology aggregates");
            createHomologyAggregates(termsByGeneId, homologene, geneGroup);
            log.info("Created {} homology aggregates", homologeneAggregateCounter);
            log.info("Created {} orthology aggregates", orthologAggregateCounter);
            log.info("Created {} top-homology aggregates, governing homologene and orthology aggregates",
                    topHomologyAggregateCounter);
            log.info("Got {} terms overall (genes and homology aggregates)", termsByGeneId.size());

            List<ImportConcept> terms = makeTermList(termsByGeneId);
            ImportFacet facet = FacetCreationService.getInstance().createFacet(importConfig);
            ImportOptions options = new ImportOptions();
            options.createHollowAggregateElements = true;
            options.doNotCreateHollowParents = true;
            ImportConcepts importConcepts = new ImportConcepts(terms, facet);
            importConcepts.setImportOptions(options);
            return Stream.of(importConcepts);
        } catch (IOException e) {
            throw new ConceptCreationException(e);
        }

    }

    /**
     * Checks if <tt>filepath</tt> is an absolute path. If so, <tt>filepath</tt> is returned. Otherweise, <tt>basepath + filepath</tt> is returned.
     *
     * @param basepath A base path to resolve the potentially relative <tt>filepath</tt> against.
     * @param filepath The - potentially relative to <tt>basepath</tt> - path to a file.
     * @return The complete path to the file pointed to by <tt>filepath</tt>.
     */
    private File resolvePath(String basepath, String filepath) {
        String delimiter = !basepath.endsWith(File.separator) && !filepath.startsWith(File.separator) ? File.separator : "";
        String path = new File(filepath).isAbsolute() ? filepath : basepath + delimiter + filepath;
        return new File(path);
    }

    @Override
    public String getName() {
        return "NCBIGeneConceptCreator";
    }

    private class TaxonomyRecord {
        @SuppressWarnings("unused")
        String taxId;
        String scientificName;
        String geneBankCommonName;

        public TaxonomyRecord(String taxId) {
            this.taxId = taxId;
        }
    }

    private class HomologeneRecord {
        @SuppressWarnings("unused")
        String taxId;
        String geneId;
        // The homology cluster ID
        String groupId;

        /**
         * From the homologene README file:
         *
         * <pre>
         * homologene.data is a tab delimited file containing the following
         * 	columns:
         * 	1) HID (HomoloGene group id)
         * 	2) Taxonomy ID
         * 	3) Gene ID
         * 	4) Gene Symbol
         * 	5) Protein gi
         * 	6) Protein accession
         * </pre>
         *
         * @author faessler
         */
        public HomologeneRecord(String[] record) {
            groupId = record[0];
            taxId = record[1];
            geneId = record[2];
        }
    }
}
