package de.julielab.concepts.db.creators;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.julielab.concepts.db.core.DefaultFacetCreator;
import de.julielab.concepts.db.core.services.FacetCreationService;
import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.java.utilities.index.IndexCreationException;
import de.julielab.java.utilities.index.PersistentLuceneIndexStringArrayMapProvider;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Label;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static java.util.stream.Collectors.toList;

public class NCBIGeneConceptCreator implements ConceptCreator {
    public static final String SEMEDICO_RESOURCE_MANAGEMENT_SOURCE = "Semedico Resource Management";
    public static final String NCBI_GENE_SOURCE = "NCBI Gene";
    public static final String BASEPATH = "basepath";
    public static final String GENE_INFO = "gene_info";
    public static final String GENEDESCRIPTIONS = "genedescriptions";
    public static final String ORGANISMLIST = "organismlist";
    public static final String ORGANISMNAMES = "organismnames";
    public static final String GENE_ORTHOLOGS = "gene_orthologs";
    public static final String UP_ID_MAPPING = "up_id_mapping";
    public static final String GENE_2_GO = "gene2go";
    public static final String GO_DB_ORIGINAL_SOURCE_NAME = "go_db_original_source_name";
    public static final String CACHE_DIR = "cache_dir";
    /**
     * "gene_group" is the name of the file specifying the ortholog relationships
     * between genes. Also, NCBI Gene, searching for a specific ortholog group works
     * by search for "ortholog_gene_2475[group]" where the number is the ID of the
     * gene that represents the group, the human gene, most of the time.
     */
    public static final String GENE_GROUP_PREFIX = "genegroup";
    public static final String TOP_ORTHOLOGY_PREFIX = "toporthology";
    public static final String TOP_HOMOLOGY_PREFIX = "tophomology";
    private Path cacheDir = Path.of("concept-manager-caches", "ncbi-gene-concepts");
    private int homologeneAggregateCounter;
    private int orthologAggregateCounter;
    private int topOrthologAggregateCounter;
    private int topHomologyAggregateCounter;
    private int uniProtConceptCounter;
    private int goConceptCounter;
    private int dbXRefCounter;
    private Logger log = LoggerFactory.getLogger(NCBIGeneConceptCreator.class);

    public NCBIGeneConceptCreator() {
        resetCounters();
    }

    public static ConceptCoordinates getGeneCoordinates(String originalId) {
        return new ConceptCoordinates(originalId, NCBI_GENE_SOURCE, originalId, NCBI_GENE_SOURCE);
    }

    private void resetCounters() {
        this.homologeneAggregateCounter = 0;
        this.orthologAggregateCounter = 0;
        this.topOrthologAggregateCounter = 0;
        this.topHomologyAggregateCounter = 0;
        this.uniProtConceptCounter = 0;
        this.goConceptCounter = 0;
    }

    /**
     * @param conceptStream
     * @param totalGeneIds
     * @param termsByGeneId
     * @param geneGroup     see https://ncbiinsights.ncbi.nlm.nih.gov/2018/02/27/gene_orthologs-file-gene-ftp/
     * @return
     * @throws IOException
     */
    private Stream<ImportConcept> createHomologyAggregates(Stream<ImportConcept> conceptStream, Set<String> totalGeneIds, Map<ConceptCoordinates, ImportConcept> termsByGeneId, File geneGroup) throws IOException {
        Multimap<String, ConceptCoordinates> genes2Aggregate = HashMultimap.create();
        Forest geneHierarchy = new Forest();

        List<String> aggregateCopyProperties = Arrays.asList(ConceptConstants.PROP_PREF_NAME, ConceptConstants.PROP_FACETS);

        Stream<ImportConcept> processedConceptStream = createGeneOrthologyAggregates(conceptStream, totalGeneIds, genes2Aggregate, geneHierarchy, geneGroup, termsByGeneId, aggregateCopyProperties);
        return createTopHomologyAggregates(processedConceptStream, geneHierarchy, genes2Aggregate, aggregateCopyProperties);
    }

    private void checkfornullparentcoords(Map<ConceptCoordinates, ImportConcept> termsByGeneId) {
        for (ImportConcept c : termsByGeneId.values()) {
            if (c.parentCoordinates == null)
                throw new IllegalArgumentException(c.coordinates.toString());
        }
    }

    private Stream<ImportConcept> createTopHomologyAggregates(Stream<ImportConcept> processedConceptStream, Forest geneHierarchy, Multimap<String, ConceptCoordinates> genes2Aggregate, List<String> aggregateCopyProperties) {
        // Now create top homology aggregates where necessary and connect the
        // top homology aggregate to the gene group and homology aggregates.
        Stream.Builder<ImportConcept> topHomologyStreamBuilder = Stream.builder();
        for (String geneId : genes2Aggregate.keySet()) {
            Optional<Node> topHomologyAggregateOpt = geneHierarchy.getRoots(getGeneCoordinates(geneId)).stream().filter(c -> c.getConcept() != null).filter(c -> c.getConcept().coordinates.sourceId.startsWith(TOP_HOMOLOGY_PREFIX)).findAny();
            if (!topHomologyAggregateOpt.isPresent() || topHomologyAggregateOpt.get().getId().sourceId.startsWith(TOP_HOMOLOGY_PREFIX)) {
                Set<ImportConcept> topAggregates = findTopOrthologsAndHomologyAggregates(geneId, geneHierarchy);
                // Only if there is more than one aggregate for the current gene we need a new top aggregate to unite the existing aggregates
                if (topAggregates.size() > 1) {
                    ImportConcept topHomologyAggregate = new ImportConcept(topAggregates.stream().map(ic -> ic.coordinates).collect(toList()), aggregateCopyProperties);
                    topHomologyAggregate.coordinates = new ConceptCoordinates();
                    topHomologyAggregate.coordinates.sourceId = TOP_HOMOLOGY_PREFIX + topHomologyAggregateCounter;
                    topHomologyAggregate.coordinates.source = SEMEDICO_RESOURCE_MANAGEMENT_SOURCE;
                    topHomologyAggregate.aggregateIncludeInHierarchy = true;
                    topHomologyAggregate.generalLabels = Arrays.asList("AGGREGATE_TOP_HOMOLOGY",
                            "NO_PROCESSING_GAZETTEER");
                    topHomologyStreamBuilder.accept(topHomologyAggregate);
                    ConceptCoordinates topHomologyCoordinates = topHomologyAggregateOpt.get().getConcept().coordinates;
//                    termsByGeneId.put(topHomologyAggregateOpt.coordinates, topHomologyAggregateOpt);
                    topAggregates.forEach(agg -> agg.addParent(topHomologyCoordinates));
                    ++topHomologyAggregateCounter;
                }
            }
        }
        return Stream.concat(processedConceptStream, topHomologyStreamBuilder.build());
    }

    private Set<ImportConcept> findTopOrthologsAndHomologyAggregates(String geneId, Forest geneHierarchy) {
        // Finds aggregates that are a gene orthology aggregate without a top orthology aggregate, are a top orthology aggregate or a homology aggregate
        Set<Node> roots = geneHierarchy.getRoots(getGeneCoordinates(geneId));
        return roots.stream().map(Node::getConcept).filter(Objects::nonNull).filter(c -> c.coordinates.sourceId.startsWith(GENE_GROUP_PREFIX)).collect(Collectors.toSet());
    }

    private Stream<ImportConcept> createGeneOrthologyAggregates(Stream<ImportConcept> conceptStream, Set<String> totalGeneIds, Multimap<String, ConceptCoordinates> genes2Aggregate, Forest geneHierarchy, File geneGroup, Map<ConceptCoordinates, ImportConcept> termsByGeneId, List<String> aggregateCopyProperties) throws IOException {
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
            String gene1 = geneGroupRecord[1].intern();
            String gene2 = geneGroupRecord[4].intern();
            geneGroupOrthologs.compute(gene1, (gene, set) -> {
                Set<String> newset = set;
                if (newset == null) newset = new HashSet<>();
                newset.add(gene2);
                return newset;
            });
        }
        log.info("Got {} orthology groups from gene_ortholog file {}", geneGroupOrthologs.size(), geneGroup);

        // 1. create separate gene orthology aggregates
        // 2. for overlapping gene orthology aggregates, create a top-orthology aggregate
        // 3. when there is a non-empty intersection between homologene and (top) gene
        // orthology aggregate elements, create a top homology aggregate
        // 4. set the new top homology aggregate as parent of the homologene and
        // (top) group aggregate nodes
        Multimap<String, ImportConcept> genes2OrthoAggregate = HashMultimap.create();
        Stream.Builder<ImportConcept> aggregatesStreamBuilder = Stream.builder();
        for (String geneGroupId : geneGroupOrthologs.keySet()) {
            Collection<String> mappingTargets = geneGroupOrthologs.get(geneGroupId);
            List<String> groupGeneIds = new ArrayList<>(mappingTargets.size() + 1);
            List<ConceptCoordinates> groupGeneCoords = new ArrayList<>(mappingTargets.size() + 1);
            // Create coordinates for this gene cluster's genes
            for (String geneId : mappingTargets) {
                // it is possible that some elements of a gene group are not in
                // our version of gene_info (e.g. due to species filtering)
                if (!totalGeneIds.contains(geneId)) {
                    continue;
                }
                groupGeneIds.add(geneId);
                groupGeneCoords.add(getGeneCoordinates(geneId));
            }
            // The gene group ID is also a valid gene. Most of the time the
            // human version. It has to be added to the resulting aggregate
            // node as well.
            // But here also we should check if we even know a gene with this ID
            if (totalGeneIds.contains(geneGroupId)) {
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
                orthologyCluster.coordinates.sourceId = (GENE_GROUP_PREFIX + geneGroupId).intern();
                orthologyCluster.coordinates.source = GENE_ORTHOLOGS;
                orthologyCluster.coordinates.originalSource = GENE_ORTHOLOGS;
                orthologyCluster.coordinates.originalId = geneGroupId;
                orthologyCluster.aggregateIncludeInHierarchy = true;
                orthologyCluster.generalLabels = Arrays.asList("AGGREGATE_GENEGROUP", "NO_PROCESSING_GAZETTEER");
//                termsByGeneId.put(orthologyCluster.coordinates,
//                        orthologyCluster);
                aggregatesStreamBuilder.accept(orthologyCluster);
                ++orthologAggregateCounter;

                for (String geneId : groupGeneIds) {
                    genes2OrthoAggregate.put(geneId, orthologyCluster);
                    ConceptCoordinates clusterCoordinates = new ConceptCoordinates(orthologyCluster.coordinates.sourceId, orthologyCluster.coordinates.source, true);
                    genes2Aggregate.put(geneId,
                            clusterCoordinates);
                    geneHierarchy.addNode(getGeneCoordinates(geneId), geneHierarchy.addNode(clusterCoordinates));
                }
            }
        }

        // Connect the genes to their orthology clusters
        conceptStream = conceptStream.map(gene -> {
            Collection<ImportConcept> orthoAggregates = genes2OrthoAggregate.get(gene.coordinates.originalId);
            for (ImportConcept orthoAggregate : orthoAggregates) {
                gene.addParent(orthoAggregate.coordinates);
                // If we actually aggregate multiple genes into one, the
                // elements should disappear behind the aggregate and as such
                // should not be present in the query dictionary or suggestions.
                if (orthoAggregates.size() > 1)
                    gene.addGeneralLabel(ConceptLabels.NO_QUERY_DICTIONARY.name(),
                            ConceptLabels.NO_SUGGESTIONS.name());
            }
            return gene;
        });

        // Create top-orthology aggregates for genes taking part in multiple orthology clusters
        Map<ConceptCoordinates, ImportConcept> orthoAgg2TopOrtho = new HashMap<>();
        for (String geneid : genes2OrthoAggregate.keySet()) {
            final Collection<ImportConcept> clusters = genes2OrthoAggregate.get(geneid);
            // If there is only one cluster associated with the current gene, we don't need to do anything here
            if (clusters.size() > 1) {
                ImportConcept topOrthologyAggregate = null;
                // Find an already existing top orthology cluster, if possible
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
//                    termsByGeneId.put(topOrthologyAggregate.coordinates,
//                            topOrthologyAggregate);
                    aggregatesStreamBuilder.accept(topOrthologyAggregate);
                    ++topOrthologAggregateCounter;
                }
                // Connect the current gene orthology clusters to the top orthology aggregate
                for (ImportConcept cluster : clusters) {
                    ConceptCoordinates clusterCoordinates = cluster.coordinates;
                    if (!topOrthologyAggregate.elementCoordinates.contains(clusterCoordinates))
                        topOrthologyAggregate.elementCoordinates.add(clusterCoordinates);
                    orthoAgg2TopOrtho.put(clusterCoordinates, topOrthologyAggregate);
                    cluster.addParent(topOrthologyAggregate.coordinates);
                    geneHierarchy.addNode(cluster.coordinates).addParent(geneHierarchy.addNode(topOrthologyAggregate.coordinates));
                }
            }
        }

        return Stream.concat(conceptStream, aggregatesStreamBuilder.build());
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

    /**
     * Gives genes species-related qualifier / display name in the form the NCBI
     * gene search engine does, e.g. interleukin 2 [Homo sapiens (human)], only that
     * we don't use the full official symbol but just the symbol to keep it a bit
     * shorter.
     *
     * @param conceptStream
     * @param ncbiTaxNames
     * @param geneId2Tax
     * @param geneTerms
     * @return
     * @throws IOException
     */
    private Stream<ImportConcept> setSpeciesQualifier(Stream<ImportConcept> conceptStream, File ncbiTaxNames, @Deprecated Map<String, String> geneId2Tax,
                                                      @Deprecated Collection<ImportConcept> geneTerms) throws IOException {
        if (ncbiTaxNames != null) {
            log.info("Setting species qualifiers from file {}", ncbiTaxNames);
            Map<String, TaxonomyRecord> taxNameRecords = new HashMap<>();
            Iterator<String> lineIt = FileUtilities.getReaderFromFile(ncbiTaxNames).lines().iterator();
            while (lineIt.hasNext()) {
                String recordString = lineIt.next();
                // at the end of the line there is no more tab, thus we have
                // actually
                // two record seperators
                String[] split = recordString.split("(\t\\|\t)|(\t\\|)");
                String taxId = split[0].intern();
                String name = split[1].intern();
                String nameClass = split[3].intern();

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

            return conceptStream.map(gene -> {
                String taxId = (String) gene.getAuxProperty("taxId");
                TaxonomyRecord taxonomyRecord = taxNameRecords.get(taxId);

                if (null != taxonomyRecord) {
                    // Set the species as a qualifier
                    String speciesQualifier = taxonomyRecord.scientificName;
                    if (null != taxonomyRecord.geneBankCommonName)
                        speciesQualifier += (" (" + taxonomyRecord.geneBankCommonName + ")").intern();
                    gene.addQualifier(speciesQualifier);

                    // Set an NCBI Gene like species-related display name.
                    gene.displayName = gene.prefName + " [" + taxonomyRecord.scientificName;
                    if (null != taxonomyRecord.geneBankCommonName)
                        gene.displayName += " (" + taxonomyRecord.geneBankCommonName + ")";
                    gene.displayName += "]";
                } else {
                    log.warn("No NCBI Taxonomy name record was found for the taxonomy ID {}", taxId);

                }

                return gene;
            });
        }
        return conceptStream;
    }

    protected Stream<ImportConcept> convertGeneInfoToImportConcepts(File geneInfo, Set<String> organismSet, File geneDescriptions) throws IOException {
        Map<String, String> gene2Summary = new HashMap<>();
        if (geneDescriptions != null) {
            log.info("Reading gene descriptions from {}", geneDescriptions);
            Iterator<String> lineIt = FileUtilities.getReaderFromFile(geneDescriptions).lines().iterator();
            while (lineIt.hasNext()) {
                String line = lineIt.next();
                String[] split = line.split("\t");
                String geneId = split[0].intern();
                String summary = split[1].intern();
                gene2Summary.put(geneId, summary);
            }
        }

        BufferedReader bw = FileUtilities.getReaderFromFile(geneInfo);
        Iterator<String> it = bw.lines().filter(record -> !record.startsWith("#")).iterator();
        Iterator<ImportConcept> geneIterator = new Iterator<>() {

            private boolean closed = false;

            @Override
            public boolean hasNext() {
                boolean hasNext = closed ? false : it.hasNext();
                if (!hasNext) {
                    try {
                        bw.close();
                        closed = true;
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
                return hasNext;
            }

            @Override
            public ImportConcept next() {
                if (hasNext()) {
                    String record = it.next();
                    ImportConcept geneconcept = createGeneConcept(record, gene2Summary);
                    String[] split = record.split("\t", 2);
                    String taxId = split[0].intern();
                    if (organismSet.contains(taxId) || organismSet.isEmpty()) {
                        geneconcept.putAuxProperty("taxId", taxId);
                        return geneconcept;
                    }
                }
                return null;
            }
        };
        log.info("Returning stream for gene concept creation.");
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(geneIterator, 0), false).filter(Objects::nonNull);
    }

    private ImportConcept createGeneConcept(String record, Map<String, String> gene2Summary) {
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
        String originalId = split[1].intern();
        String synonymString = split[4];
        String otherDesignations = split[13];
        // synonyms:
        // 1. official full name (if not used as preferred name)
        // 2. synonyms
        // 3. other designations
        String[] synonymSplit = synonymString.split("\\|");
        for (int i = 0; i < synonymSplit.length; i++) {
            String synonym = synonymSplit[i].intern();
            synonyms.add(synonym);
        }
        String[] otherDesignationsSplit = otherDesignations.split("\\|");
        for (int i = 0; i < otherDesignationsSplit.length; i++) {
            String synonym = otherDesignationsSplit[i];
            synonyms.add(synonym.intern());
        }
        String description = gene2Summary.get(originalId);
        if (description != null)
            description = description.intern();

        // remove synonyms that are too short
        for (Iterator<String> synonymIt = synonyms.iterator(); synonymIt.hasNext(); ) {
            if (synonymIt.next().length() < 2)
                synonymIt.remove();
        }
        ImportConcept geneTerm = new ImportConcept(prefName, synonyms, description,
                getGeneCoordinates(originalId));
        geneTerm.additionalProperties = new HashMap<>();
        geneTerm.additionalProperties.put("taxId", split[0]);
        // this property is meant to be read - and removed - in createDbXRefMappings()
        geneTerm.additionalProperties.put("dbXrefs", split[5]);

        /**
         * Gene IDs are given by a Gene Normalization component like GeNo. Thus, genes
         * are not supposed to be additionally tagged by a gazetteer.
         */
        geneTerm.addGeneralLabel(ConceptLabels.NO_PROCESSING_GAZETTEER.toString(),
                ConceptLabels.ID_MAP_NCBI_GENES.toString());

        return geneTerm;

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
        template.addProperty(slash(base, GENE_ORTHOLOGS), "");
        template.addProperty(slash(base, UP_ID_MAPPING), "");
        template.addProperty(slash(base, GENE_2_GO), "");
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
            ConfigurationUtilities.checkParameters(importConfig, slash(confPath, GENE_INFO), slash(confPath, ORGANISMLIST),
                    slash(confPath, GENE_ORTHOLOGS));
        } catch (ConfigurationException e) {
            throw new ConceptCreationException(e);
        }

        String basepath = importConfig.getString(slash(confPath, BASEPATH), "");
        File geneInfo = resolvePath(basepath, importConfig.getString(slash(confPath, GENE_INFO)));
        File geneDescriptions = resolvePath(basepath, importConfig.getString(slash(confPath, GENEDESCRIPTIONS)));
        File organisms = resolvePath(basepath, importConfig.getString(slash(confPath, ORGANISMLIST)));
        File ncbiTaxNames = resolvePath(basepath, importConfig.getString(slash(confPath, ORGANISMNAMES)));
        File geneOrthologs = resolvePath(basepath, importConfig.getString(slash(confPath, GENE_ORTHOLOGS)));
        File uniprotIdMapping = resolvePath(basepath, importConfig.getString(slash(confPath, UP_ID_MAPPING)));
        File gene2go = resolvePath(basepath, importConfig.getString(slash(confPath, GENE_2_GO)));
        String goOriginalSourceName = importConfig.getString(slash(confPath, GO_DB_ORIGINAL_SOURCE_NAME));
        File cacheDirFile = resolvePath(basepath, importConfig.getString(slash(confPath, CACHE_DIR)));


        try {
            if (cacheDirFile != null) {
                log.info("Setting cache directory to {}", cacheDirFile);
                cacheDir = cacheDirFile.toPath();
                if (!Files.exists(cacheDir))
                    Files.createDirectories(cacheDir);
            }
            log.info("Beginning import of NCBI Genes.");
            if (organisms != null)
                log.info("Reading the set of organisms to import genes of from {}.", organisms);
            Set<String> organismSet = organisms != null ? FileUtilities.getReaderFromFile(organisms).lines().map(String::intern).collect(Collectors.toSet()) : Collections.emptySet();
            if (!organismSet.isEmpty())
                log.info("Retrieved {} taxonomy IDs.", organismSet.size());
            else
                log.info("Retrieved {} taxonomy IDs. No restrictions on species is imposed.", organismSet.size());
            log.info("Reading the set of valid gene IDs from {}.", geneInfo);
            Set<String> totalGeneIds = getTotalGeneIds(geneInfo, organismSet);
            log.info("Got {} gene IDs.", totalGeneIds.size());
            Map<String, String> geneId2Tax = new HashMap<>();
            Map<ConceptCoordinates, ImportConcept> conceptsByGeneId = new HashMap<>();
            log.info("Creating a stream converting NCBI Gene's gene_info file into nodes for the concept graph.");
            Stream<ImportConcept> conceptStream = convertGeneInfoToImportConcepts(geneInfo, organismSet, geneDescriptions);
            conceptStream = setSpeciesQualifier(conceptStream, ncbiTaxNames, geneId2Tax, conceptsByGeneId.values());
            conceptStream = createUniProtIdMappings(conceptStream, uniprotIdMapping, totalGeneIds);
            conceptStream = createDbXRefMappings(conceptStream);
            conceptStream = createGoAnnotationLinks(conceptStream, gene2go, goOriginalSourceName, totalGeneIds);
            log.info("Creating homology aggregates");
            conceptStream = createHomologyAggregates(conceptStream, totalGeneIds, conceptsByGeneId, geneOrthologs);
            log.info("Created {} homology aggregates", homologeneAggregateCounter);
            log.info("Created {} orthology aggregates", orthologAggregateCounter);
            log.info("Created {} top-homology aggregates, governing homologene and orthology aggregates",
                    topHomologyAggregateCounter);

            ImportFacet facet = FacetCreationService.getInstance().createFacet(importConfig);
            ImportOptions options = new ImportOptions();
            options.createHollowAggregateElements = true;
            // This was "true" in the past. Hopefully we can make it work anyway.
            options.doNotCreateHollowParents = false;
            ImportConcepts importConcepts = new ImportConcepts(conceptStream, facet);
            importConcepts.setNumConcepts(totalGeneIds.size() + uniProtConceptCounter + homologeneAggregateCounter + orthologAggregateCounter + topHomologyAggregateCounter + dbXRefCounter);
            log.info("Created a total of {} concepts.", importConcepts.getNumConcepts());
            importConcepts.setImportOptions(options);
            return Stream.of(importConcepts);
        } catch (IOException | IndexCreationException e) {
            throw new ConceptCreationException(e);
        }

    }

    private Stream<ImportConcept> createGoAnnotationLinks(Stream<ImportConcept> conceptStream, File gene2go, String goOriginalSourceName, Set<String> totalGeneIds) throws IOException {
        if (gene2go != null) {
            if (StringUtils.isBlank(goOriginalSourceName))
                throw new IllegalArgumentException("Found GO gene annotation file. But the " + GO_DB_ORIGINAL_SOURCE_NAME + " parameter is not given. It needs to be set to the original source used by the concept creator that has imported the GO terms. When in doubt, use the GO concept importer individually and then check the database for the source name.");
            Multimap<String, String[]> geneAnnotations = HashMultimap.create();
            log.info("Reading gene GO annotations from {} while excluding qualifiers beginning with NOT.", gene2go);
            int numAnnotations = 0;
            try (final BufferedReader br = FileUtilities.getReaderFromFile(gene2go)) {
                final Iterator<String> lineIt = br.lines().iterator();
                while (lineIt.hasNext()) {
                    String line = lineIt.next();
                    if (line.startsWith("#"))
                        continue;
                    // #tax_id GeneID  GO_ID   Evidence        Qualifier       GO_term PubMed  Category
                    // 3702    814630  GO:0003700      ISS     enables DNA-binding transcription factor activity       11118137        Function
                    // 3702    814652  GO:0005794      RCA     NOT located_in  Golgi apparatus 22430844        Component
                    final String[] split = line.split("\\t", 6);
                    String geneId = split[1].intern();
                    if (totalGeneIds.contains(geneId)) {
                        String goId = split[2];
                        String qualifier = split[4];
                        if (!qualifier.startsWith("NOT")) {
                            geneAnnotations.put(geneId, new String[]{goId, qualifier});
                            ++numAnnotations;
                        }
                    }
                }
            }
            log.info("Received {} GO annotations for {} genes.", numAnnotations, geneAnnotations.keySet().size());
            return conceptStream.map(concept -> {
                        if (concept.generalLabels != null && concept.generalLabels.contains(ConceptLabels.ID_MAP_NCBI_GENES.name())) {
                            final Collection<String[]> annotations = geneAnnotations.get(concept.coordinates.originalId);
                            if (annotations != null) {
                                log.trace("Retrieved GO annotation {} for gene {}", annotations, concept.coordinates.originalId);
                                for (String[] annotation : annotations) {
                                    String goId = annotation[0];
                                    String qualifier = annotation[1];
                                    final ImportConceptRelationship annotatedWith = new ImportConceptRelationship(new ConceptCoordinates(goId, goOriginalSourceName, CoordinateType.OSRC), "IS_ANNOTATED_WITH");
                                    annotatedWith.addProperty("qualifier", qualifier);
                                    concept.addRelationship(annotatedWith);
                                }
                            }
                        }
                        return concept;
                    }
            );
        }
        return conceptStream;
    }

    private Stream<ImportConcept> createDbXRefMappings(Stream<ImportConcept> conceptStream) {
        log.info("Adding dbXref items to the concept stream.");
        return conceptStream.flatMap(concept -> {
            List<ImportConcept> returnedConcepts = new ArrayList<>();
            returnedConcepts.add(concept);
            // this concept could also be a non-ncbi-gene-concept but UniProt for the UniProt ID mapping
            if (concept.generalLabels != null && concept.generalLabels.contains(ConceptLabels.ID_MAP_NCBI_GENES.name())) {
                // we read this property from gene_info in createGeneConcept()
                final String dbXrefsString = (String) concept.additionalProperties.get("dbXrefs");
                concept.additionalProperties.remove("dbXrefs");
                final String[] dbXrefs = dbXrefsString.split("\\|");
                for (String dbXref : dbXrefs) {
                    String refId = null;
                    String refSource = null;
                    String refLabel = null;
                    // All items read here need to be taken into account in getTotalGeneIds() for the count of concepts reported to the concept importer
                    if (dbXref.startsWith("Ensembl:")) {
                        refId = dbXref.substring(8);
                        refSource = "Ensembl";
                        refLabel = "ENSEMBL";
                    } else if (dbXref.startsWith("HGNC:")) {
                        refId = dbXref.substring(5);
                        refSource = "HGNC";
                        refLabel = "HGNC";
                    }
                    if (refId != null) {
                        final ImportConcept refConcept = new ImportConcept(new ConceptCoordinates(refId, refSource, refId, refSource));
                        log.trace("Creating dbXref concept with coordinates {}", refConcept.coordinates);
                        refConcept.addGeneralLabel(refLabel);
                        refConcept.eligibleForFacetRoot = false;
                        refConcept.addRelationship(new ImportConceptRelationship(concept.coordinates, "IS_MAPPED_TO"));
                        returnedConcepts.add(refConcept);
                    }
                }
            }
            return returnedConcepts.stream();
        });
    }

    private Stream<ImportConcept> createUniProtIdMappings(Stream<ImportConcept> conceptStream, File uniprotIdMappingFile, Set<String> totalGeneIds) throws IOException, IndexCreationException {
        if (uniprotIdMappingFile != null) {
            log.info("Creating index for UniProt ID mapping file {} to save memory.", uniprotIdMappingFile);
            final PersistentLuceneIndexStringArrayMapProvider mappingIndex = new PersistentLuceneIndexStringArrayMapProvider();
            mappingIndex.setIndexDirectoryPath(cacheDir);
            mappingIndex.setEligibleKeys(totalGeneIds);
            // in the UniProt mapping file, gene IDs appear in the third column
            mappingIndex.setKeyIndices(2);
            // the first column is the UP Accession, the second the mnemonic
            mappingIndex.setValueIndices(0, 1);
            mappingIndex.load(uniprotIdMappingFile.toURI());
            final Map<String, String[]> indexMap = mappingIndex.getMap();
            uniProtConceptCounter += indexMap.size();
            Set<String> seenUpAcs = new HashSet<>();
            log.info("Creating updated gene concept stream with additional IDs.");
            return conceptStream.flatMap(concept -> {
                List<ImportConcept> returnedConcepts = new ArrayList<>();
                returnedConcepts.add(concept);
                // Not every gene ID corresponds to some UniProt entry
                final String[] upIds = indexMap.get(concept.coordinates.originalId);
                if (upIds != null) {
                    if (upIds.length % 2 == 1)
                        throw new IllegalStateException("An uneven number of UniProt ACs/IDs was returned but there should be pairs of ACs and IDs.");

                    for (int i = 1; i < upIds.length; i++) {
                        String upAc = upIds[i-1];
                        if (seenUpAcs.add(upAc)) {
                            String upId = upIds[i];
                            final ImportConcept upConcept = new ImportConcept(new ConceptCoordinates(upAc, "UniProtKB-AC", upAc, "UniProtKB-AC"));
                            upConcept.addGeneralLabel("UNIPROT");
                            upConcept.eligibleForFacetRoot = false;
                            upConcept.addAdditionalCoordinates(new ConceptCoordinates( upId,"UniProtKB-ID", CoordinateType.SRC));
                            upConcept.addAdditionalProperty("UniProtKB-ID", upId);
                            upConcept.addRelationship(new ImportConceptRelationship(concept.coordinates, "IS_MAPPED_TO"));
                            returnedConcepts.add(upConcept);
                        }
                    }
                }

                return returnedConcepts.stream();
            });
        } else {
            log.info("UniProt ID mapping file not specified or not found, skipping UniProt ID mappings.");
        }
        return conceptStream;
    }

    private Set<String> getTotalGeneIds(File geneInfo, Set<String> organismSet) throws IOException {
        Set<String> geneIdSet = Collections.emptySet();
        Path genesetCacheFile = Path.of(cacheDir.toString(), "totalGeneIds.ser.gz");
        boolean geneInfoNewerThanCache = Files.exists(genesetCacheFile) && geneInfo.lastModified() > Files.getLastModifiedTime(genesetCacheFile).toMillis();
        if (geneInfoNewerThanCache) {
            log.info("gene_info file at {} is newer than cache at {}. Clearing cache and reading gene_info file from scratch.", geneInfoNewerThanCache, genesetCacheFile);
            FileUtils.deleteQuietly(genesetCacheFile.toFile());
        }
        boolean readFromCache = Files.exists(genesetCacheFile) && !geneInfoNewerThanCache;
        if (readFromCache) {
            log.info("Loading set of gene IDs in gene_info from cache at {}", genesetCacheFile);
            try (final ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(FileUtilities.getInputStreamFromFile(genesetCacheFile.toFile())))) {
                geneIdSet = (Set<String>) ois.readObject();
                dbXRefCounter = (int) ois.readObject();
                log.info("Obtained {} gene IDs and {} dbXref IDs in the cache.", geneIdSet.size(), dbXRefCounter);
            } catch (ClassNotFoundException e) {
                // this should really not happen with Java built-in classes
                log.error("Unexpected error when trying to read gene ID set cache from {}. Perhaps the cache is corrupt. Trying to delete it and start again.");
                FileUtils.deleteQuietly(genesetCacheFile.toFile());
                readFromCache = true;
            }
        }
        if (!readFromCache) {
            try (BufferedReader bw = FileUtilities.getReaderFromFile(geneInfo)) {
                final AtomicInteger dbXrefAtomicCounter = new AtomicInteger();
                geneIdSet = bw.lines()
                        .filter(record -> !record.startsWith("#"))
                        .map(record -> record.split("\t", 6))
                        .filter(split -> organismSet.contains(split[0]) || organismSet.isEmpty())
                        // This list of dbXref items must correspond to the items actually read in createDbXRefMappings()
                        .peek(split -> dbXrefAtomicCounter.addAndGet((int) Arrays.stream(split[5].split("\\|")).filter(dbXref -> dbXref.startsWith("Ensembl:") || dbXref.startsWith("HGNC:")).count()))
                        .map(split -> split[1].intern())
                        .collect(Collectors.toSet());
                dbXRefCounter += dbXrefAtomicCounter.get();
                log.info("Received {} dbXref IDs which will result in additional concepts for the ID mapping", dbXRefCounter);
            }
            try (final ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(FileUtilities.getOutputStreamToFile(genesetCacheFile.toFile())))) {
                log.info("Caching geneId set read from {} at {}", geneInfo, genesetCacheFile);
                oos.writeObject(geneIdSet);
                oos.writeObject(dbXRefCounter);
            }
        }
        return geneIdSet;
    }

    /**
     * Checks if <tt>filepath</tt> is an absolute path. If so, <tt>filepath</tt> is returned. Otherwise, <tt>basepath + filepath</tt> is returned.
     *
     * @param basepath A base path to resolve the potentially relative <tt>filepath</tt> against.
     * @param filepath The - potentially relative to <tt>basepath</tt> - path to a file.
     * @return The complete path to the file pointed to by <tt>filepath</tt>.
     */
    private File resolvePath(String basepath, String filepath) {
        if (StringUtils.isBlank(filepath))
            return null;
        String delimiter = !StringUtils.isBlank(basepath) && !basepath.endsWith(File.separator) && !filepath.startsWith(File.separator) ? File.separator : "";
        String path = new File(filepath).isAbsolute() ? filepath : basepath + delimiter + filepath;
        return new File(path);
    }

    @Override
    public String getName() {
        return "NCBIGeneConceptCreator";
    }

    // These constants were used to be imported from import de.julielab.semedico.resources.ResourceTermLabels
    // This connection was loosened for less cumbersome dependencies.
    public enum ConceptLabels implements Label {NO_PROCESSING_GAZETTEER, NO_SUGGESTIONS, NO_QUERY_DICTIONARY, ID_MAP_NCBI_GENES}

    private class TaxonomyRecord {
        @SuppressWarnings("unused")
        String taxId;
        String scientificName;
        String geneBankCommonName;

        public TaxonomyRecord(String taxId) {
            this.taxId = taxId.intern();
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
            groupId = record[0].intern();
            taxId = record[1].intern();
            geneId = record[2].intern();
        }
    }
}
