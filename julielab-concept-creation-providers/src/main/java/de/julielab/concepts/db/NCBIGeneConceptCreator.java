package de.julielab.concepts.db;

import static java.util.stream.Collectors.joining;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import de.julielab.concepts.db.core.services.FacetCreationService;
import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportOptions;
import de.julielab.neo4j.plugins.datarepresentation.TermCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.semedico.resources.ResourceTermLabels;

public class NCBIGeneConceptCreator implements ConceptCreator {

	public static final String SEMEDICO_RESOURCE_MANAGEMENT_SOURCE = "Semedico Resource Management";
	public static final String NCBI_GENE_SOURCE = "NCBI Gene";
	public String HOMOLOGENE_PREFIX = "homologene";
	/**
	 * "gene_group" is the name of the file specifying the ortholog relationships
	 * between genes. Also, NCBI Gene, searching for a specific ortholog group works
	 * by search for "ortholog_gene_2475[group]" where the number is the ID of the
	 * gene that represents the group, the human gene, most of the time.
	 */
	public String GENE_GROUP_PREFIX = "genegroup";
	public String TOP_HOMOLOGY_PREFIX = "tophomology";
	private int homologeneAggregateCounter;
	private int orthologAggregateCounter;
	private int topHomologyAggregateCounter;
	private Logger log = LoggerFactory.getLogger(NCBIGeneConceptCreator.class);

	public NCBIGeneConceptCreator() {
		this.homologeneAggregateCounter = 0;
		this.orthologAggregateCounter = 0;
		this.topHomologyAggregateCounter = 0;
	}

	/**
	 * 
	 * @param termsByGeneId
	 * @param homologene
	 * @param geneGroup
	 *            see http://www.ncbi.nlm.nih.gov/news/03-13-2014-gene-provides-
	 *            orthologs-regions/
	 * @throws IOException
	 */
	private void createHomologyAggregates(Map<TermCoordinates, ImportConcept> termsByGeneId, File homologene,
			File geneGroup) throws IOException {
		Multimap<String, TermCoordinates> genes2Aggregate = HashMultimap.create();

		List<String> aggregateCopyProperties = Arrays.asList(ConceptConstants.PROP_PREF_NAME,
				ConceptConstants.PROP_SYNONYMS, ConceptConstants.PROP_WRITING_VARIANTS,
				ConceptConstants.PROP_DESCRIPTIONS, ConceptConstants.PROP_FACETS);
		Multimap<String, HomologeneRecord> groupId2Homolo = HashMultimap.create();
		LineIterator iterator = FileUtils.lineIterator(homologene);
		while (iterator.hasNext()) {
			String line = (String) iterator.next();
			String[] split = line.split("\t");
			if (split.length != 6)
				throw new IllegalStateException("Expected 6 fields in homologene file format, but got " + split.length
						+ " in line \"" + line + "\"");
			HomologeneRecord record = new HomologeneRecord(split);
			groupId2Homolo.put(record.groupId, record);
		}

		// create homologene aggregates
		Map<String, String> genes2HomoloGroup = new HashMap<>();
		for (String groupId : groupId2Homolo.keySet()) {
			Collection<HomologeneRecord> group = groupId2Homolo.get(groupId);
			List<String> homologuousGeneIds = new ArrayList<>(group.size());
			List<String> homologuousGeneSources = new ArrayList<>(group.size());
			List<TermCoordinates> homologuousGeneCoords = new ArrayList<>(group.size());
			for (HomologeneRecord record : group) {
				String geneId = record.geneId;
				TermCoordinates geneCoords = new TermCoordinates(geneId, NCBI_GENE_SOURCE);
				if (!termsByGeneId.containsKey(geneCoords))
					continue;
				homologuousGeneIds.add(geneId);
				homologuousGeneSources.add(NCBI_GENE_SOURCE);
				homologuousGeneCoords.add(geneCoords);
			}
			ImportConcept aggregate = new ImportConcept(homologuousGeneCoords, aggregateCopyProperties);
			aggregate.coordinates = new ConceptCoordinates();
			aggregate.coordinates.sourceId = HOMOLOGENE_PREFIX + groupId;
			aggregate.coordinates.source = "Homologene";
			aggregate.aggregateIncludeInHierarchy = true;
			aggregate.generalLabels = Arrays.asList("AGGREGATE_HOMOLOGENE", "NO_PROCESSING_GAZETTEER");
			aggregate.coordinates.originalSource = "Homologene";
			aggregate.coordinates.originalId = groupId;
			termsByGeneId.put(new TermCoordinates(aggregate.coordinates.sourceId, aggregate.coordinates.source),
					aggregate);
			++homologeneAggregateCounter;

			for (TermCoordinates geneCoords : homologuousGeneCoords) {
				String geneId = geneCoords.id;
				ImportConcept gene = termsByGeneId.get(geneCoords);
				if (genes2HomoloGroup.containsKey(geneId))
					throw new IllegalStateException(
							"Gene with ID " + geneId + " is taking part in multiple homologene groups.");
				genes2HomoloGroup.put(geneId, groupId);
				genes2Aggregate.put(geneId,
						new TermCoordinates(aggregate.coordinates.sourceId, aggregate.coordinates.source));
				gene.addParent(aggregate.coordinates);
				// gene.addParentSrcId(aggregate.coordinates.sourceId);
				// If we actually aggregate multiple genes into one, the
				// elements should disappear behind the aggregate and as such
				// should not be present in the query dictionary or suggestions.

				if (homologuousGeneIds.size() > 1) {
					gene.addGeneralLabel(ResourceTermLabels.Gazetteer.NO_QUERY_DICTIONARY.name(),
							ResourceTermLabels.Suggestions.NO_SUGGESTIONS.name());
				}
			}
		}
		// and homologene aggregates

		// add the orthology information from gene group to make gene group aggregates
		Multimap<String, String> geneGroupOrthologs = HashMultimap.create();
		iterator = FileUtils.lineIterator(geneGroup);
		// Format: tax_id GeneID relationship Other_tax_id Other_GeneID (tab is
		// used as a separator, pound sign - start of a comment)
		while (iterator.hasNext()) {
			String geneGroupLine = (String) iterator.next();
			if (geneGroupLine.startsWith("#"))
				continue;
			String[] geneGroupRecord = geneGroupLine.split("\t");
			String relationship = geneGroupRecord[2];
			if (!relationship.equals("Ortholog"))
				continue;
			String gene1 = geneGroupRecord[1];
			String gene2 = geneGroupRecord[4];
			geneGroupOrthologs.put(gene1, gene2);
		}

		// 1. create separate gene group aggregates
		// 2. when there are non-empty intersection between homologene and gene
		// group aggregate elements, create a top homology aggregate
		// 3. set the new top homology aggregate as parent of the homologene and
		// group aggregate nodes
		Map<String, String> genes2OrthoGroup = new HashMap<>();
		for (String geneGroupId : geneGroupOrthologs.keySet()) {
			Collection<String> mappingTargets = geneGroupOrthologs.get(geneGroupId);
			List<String> groupGeneIds = new ArrayList<>(mappingTargets.size() + 1);
			List<TermCoordinates> groupGeneCoords = new ArrayList<>(mappingTargets.size() + 1);
			for (String geneId : mappingTargets) {
				// it is possible that some elements of a gene group are not in
				// our version of gene_info (e.g. due to species filtering)
				TermCoordinates geneCoords = new TermCoordinates(geneId, NCBI_GENE_SOURCE);
				if (!termsByGeneId.containsKey(geneCoords))
					continue;
				groupGeneIds.add(geneId);
				groupGeneCoords.add(geneCoords);
			}
			// The gene group ID is also a valid gene. Most of the time the
			// human version. It has to be added to the resulting aggregate
			// node, as well.
			// But here also we should check if we even know a gene with this ID
			if (termsByGeneId.containsKey(new TermCoordinates(GENE_GROUP_PREFIX + geneGroupId, "GeneGroup"))) {
				groupGeneIds.add(geneGroupId);
				groupGeneCoords.add(new TermCoordinates(geneGroupId, NCBI_GENE_SOURCE));
			}

			ImportConcept aggregate = new ImportConcept(groupGeneCoords, aggregateCopyProperties);
			aggregate.coordinates = new ConceptCoordinates();
			aggregate.coordinates.sourceId = GENE_GROUP_PREFIX + geneGroupId;
			aggregate.coordinates.source = "GeneGroup";
			aggregate.coordinates.originalSource = "GeneGroup";
			aggregate.coordinates.originalId = geneGroupId;
			aggregate.aggregateIncludeInHierarchy = true;
			aggregate.generalLabels = Arrays.asList("AGGREGATE_GENEGROUP", "NO_PROCESSING_GAZETTEER");
			termsByGeneId.put(new TermCoordinates(aggregate.coordinates.sourceId, aggregate.coordinates.source),
					aggregate);
			++orthologAggregateCounter;

			for (String geneId : groupGeneIds) {
				ImportConcept gene = termsByGeneId.get(new TermCoordinates(geneId, NCBI_GENE_SOURCE));
				// it can happen that gene_group lists a gene we do not work
				// with (e.g. since we filter for a subset of organisms)
				// outcommented: should now already be handled above
				// if (null == gene)
				// continue;
				if (genes2OrthoGroup.containsKey(geneId))
					throw new IllegalStateException(
							"Gene with ID " + geneId + " is taking part in multiple ortholog gene groups.");
				genes2OrthoGroup.put(geneId, geneGroupId);
				genes2Aggregate.put(geneId,
						new TermCoordinates(aggregate.coordinates.sourceId, aggregate.coordinates.source));
				gene.addParent(aggregate.coordinates);
				// If we actually aggregate multiple genes into one, the
				// elements should disappear behind the aggregate and as such
				// should not be present in the query dictionary or suggestions.

				if (groupGeneIds.size() > 1) {
					gene.addGeneralLabel(ResourceTermLabels.Gazetteer.NO_QUERY_DICTIONARY.name(),
							ResourceTermLabels.Suggestions.NO_SUGGESTIONS.name());
				}
			}
		}

		// Now create top homology aggregates where necessary and connect the
		// top homology aggregate to the gene group and homology aggregates.
		for (String geneId : genes2Aggregate.keySet()) {
			Collection<TermCoordinates> aggregateCoords = genes2Aggregate.get(geneId);
			// we only create a top homology node if the gene is element of a
			// gene group and a homology aggregate
			if (aggregateCoords.size() > 1) {
				// First check if these aggregates already have a top-aggregate
				int hasNoTopAggregate = 0;
				TermCoordinates existingAggregateCoords = null;
				for (TermCoordinates aggregateCoord : aggregateCoords) {
					ImportConcept aggregate = termsByGeneId.get(aggregateCoord);
					if (!aggregate.hasParents())
						++hasNoTopAggregate;
					else if (aggregate.parentCoordinates.size() == 1)
						existingAggregateCoords = new TermCoordinates(aggregate.parentCoordinates.get(0).sourceId,
								aggregate.parentCoordinates.get(0).source);
					else
						throw new IllegalStateException("The aggregate with ID " + aggregateCoord.id
								+ " has multiple parents: " + aggregate.parentCoordinates.stream().map(c -> c.sourceId)
										.collect(Collectors.toList()));
				}
				// just some information
				if (hasNoTopAggregate > 0 && hasNoTopAggregate != aggregateCoords.size())
					log.trace("The gene concept " + geneId + " is element of " + aggregateCoords.size()
							+ " aggregates. However, at least one of those aggregates already is part of a top aggregate while at least one other is not. This means in this case, gene group is not a strict superset of homologene. The already found top aggregate will be used to represent also this term. Aggregates of this term are: "
							+ aggregateCoords);
				// no top homology aggregate missing, continue with the next
				// gene
				else if (hasNoTopAggregate == 0)
					continue;

				ImportConcept topHomologyAggregate;
				if (null == existingAggregateCoords) {
					topHomologyAggregate = new ImportConcept(new ArrayList<>(aggregateCoords), aggregateCopyProperties);
					topHomologyAggregate.coordinates = new ConceptCoordinates();
					topHomologyAggregate.coordinates.sourceId = TOP_HOMOLOGY_PREFIX + topHomologyAggregateCounter;
					topHomologyAggregate.coordinates.source = SEMEDICO_RESOURCE_MANAGEMENT_SOURCE;
					topHomologyAggregate.aggregateIncludeInHierarchy = true;
					topHomologyAggregate.generalLabels = Arrays.asList("AGGREGATE_TOP_HOMOLOGY",
							"NO_PROCESSING_GAZETTEER");
					termsByGeneId.put(new TermCoordinates(topHomologyAggregate.coordinates.sourceId,
							topHomologyAggregate.coordinates.source), topHomologyAggregate);
					++topHomologyAggregateCounter;
				} else {
					topHomologyAggregate = termsByGeneId.get(existingAggregateCoords);
					// add the new aggregate(s) that also should be governed by
					// the top aggregate
					for (TermCoordinates aggregateCoord : aggregateCoords) {
						if (!topHomologyAggregate.elementCoordinates.contains(aggregateCoord)) {
							topHomologyAggregate.elementCoordinates.add(aggregateCoord);
						}
					}
				}
				for (TermCoordinates aggregateCoord : aggregateCoords) {
					ImportConcept aggregate = termsByGeneId.get(aggregateCoord);
					aggregate.addParentIfNotExists(topHomologyAggregate.coordinates);
					// parentSrcIds =
					// Collections.singletonList(topHomologyAggregate.coordinates.sourceId);
					// aggregate.parentSources =
					// Collections.singletonList(topHomologyAggregate.coordinates.source);
					aggregate.addGeneralLabel(ResourceTermLabels.Gazetteer.NO_QUERY_DICTIONARY.name(),
							ResourceTermLabels.Suggestions.NO_SUGGESTIONS.name());
				}
			}
		}
	}

	private List<ImportConcept> makeTermList(Map<TermCoordinates, ImportConcept> termsByGeneId) {
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
		LineIterator lineIt = FileUtils.lineIterator(ncbiTaxNames, "UTF-8");
		while (lineIt.hasNext()) {
			String recordString = lineIt.nextLine();
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
			Map<String, String> geneId2Tax, Map<TermCoordinates, ImportConcept> termsByGeneId) throws IOException {
		Set<String> organismSet = new HashSet<>(IOUtils.readLines(new FileInputStream(organisms), "UTF-8"));

		LineIterator lineIt = FileUtils.lineIterator(geneDescriptions, "UTF-8");
		Map<String, String> gene2Summary = new HashMap<>();
		while (lineIt.hasNext()) {
			String line = lineIt.nextLine();
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
				if (organismSet.contains(taxId)) {
					geneId2Tax.put(term.coordinates.originalId, taxId);
					termsByGeneId.put(new TermCoordinates(term.coordinates.originalId, term.coordinates.originalSource),
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
		for (Iterator<String> synonymIt = synonyms.iterator(); synonymIt.hasNext();) {
			if (synonymIt.next().length() < 2)
				synonymIt.remove();
		}
		ImportConcept geneTerm = new ImportConcept(prefName, synonyms, description,
				new ConceptCoordinates(originalId, NCBI_GENE_SOURCE, originalId, NCBI_GENE_SOURCE));
		/**
		 * Gene IDs are given by a Gene Normalization component like GeNo. Thus, genes
		 * are not supposed to be additionally tagged by a gazetteer.
		 */
		geneTerm.addGeneralLabel(ResourceTermLabels.Gazetteer.NO_PROCESSING_GAZETTEER.toString(),
				ResourceTermLabels.IdMapping.ID_MAP_NCBI_GENES.toString());
		// geneTerm.coordinates.originalId = originalId;
		// TODO This should be some kind of constant or identifier to a
		// "Source Node" in the database having all
		// information about the actual source so it could be displayed in
		// Semedico.
		// geneTerm.coordinates.originalSource = NCBI_GENE_SOURCE;
		// geneTerm.coordinates.source = NCBI_GENE_SOURCE;

		return geneTerm;

	}

	private class TaxonomyRecord {
		public TaxonomyRecord(String taxId) {
			this.taxId = taxId;
		}

		@SuppressWarnings("unused")
		String taxId;
		String scientificName;
		String geneBankCommonName;
	}

	private class HomologeneRecord {
		/**
		 * From the homologene README file:
		 * 
		 * <pre>
		 * homologene.data is a tab delimited file containing the following
		*	columns:
		*	1) HID (HomoloGene group id)
		*	2) Taxonomy ID
		*	3) Gene ID
		*	4) Gene Symbol
		*	5) Protein gi
		*	6) Protein accession
		 * </pre>
		 * 
		 * @author faessler
		 *
		 */
		public HomologeneRecord(String[] record) {
			groupId = record[0];
			taxId = record[1];
			geneId = record[2];
		}

		@SuppressWarnings("unused")
		String taxId;
		String geneId;
		// The homology cluster ID
		String groupId;
	}

	public static final String CONFKEY_GENE_INFO = "configuration.gene_info";
	public static final String CONFKEY_GENE_DESCRIPTIONS = "configuration.genedescriptions";
	public static final String CONFKEY_ORGANISMS = "configuration.organismlist";
	public static final String CONFKEY_ORGANISMS_NAMES = "configuration.organismnames";
	public static final String CONFKEY_HOMOLOGENE = "configuration.homologene";
	public static final String CONFKEY_GENE_GROUP = "configuration.gene_group";

	/**
	 * 
	 * @param geneInfo
	 *            Original gene_info file download from the NCBI. Should reside on
	 *            our servers at
	 *            <tt>/data/data_resources/biology/entrez/gene/gene_info</tt> (or
	 *            similar, path could change over time).
	 * @param organisms
	 *            A list of NCBI Taxonomy IDs specifying the organisms for which
	 *            genes should be included. The whole of the gene database contains
	 *            around 16M entries, as of August 2014, most of which do not stand
	 *            in the focus of research. The list given here should be the same
	 *            list used for GeNo resource generation (organisms.taxid) to create
	 *            a match between terms in the term database and actually mapped
	 *            genes in the documents.
	 * @param ncbiTaxNames
	 *            The <tt>names.dmp</tt> file included in the original NCBI Taxonomy
	 *            download. Should reside on our servers at
	 *            <tt>/data/data_resources/biology/ncbi_tax/names.dmp</tt> (or
	 *            similar, path could change over time).
	 * @param geneSummary
	 *            This file - unfortunately - cannot be downloaded directly.
	 *            However, it should already exist, somewhere, since it is part of
	 *            GeNo resource generation. You can either ask someone who is
	 *            responsible for GeNo, or just build the semantic context index
	 *            yourself with the script that is included in the
	 *            jules-gene-mapper-ae project. Please note that summary download
	 *            takes a while (a few hours) and thus is filtered to only download
	 *            summaries for the genes that are included in GeNo.
	 * @param homologene
	 * @throws FacetCreationException
	 * @throws IOException
	 */
	@Override
	public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig)
			throws ConceptCreationException, FacetCreationException {
		checkParameters(importConfig, CONFKEY_GENE_INFO, CONFKEY_GENE_DESCRIPTIONS, CONFKEY_ORGANISMS,
				CONFKEY_ORGANISMS_NAMES, CONFKEY_HOMOLOGENE, CONFKEY_GENE_GROUP);

		File geneInfo = new File(importConfig.getString(CONFKEY_GENE_INFO));
		File geneDescriptions = new File(importConfig.getString(CONFKEY_GENE_DESCRIPTIONS));
		File organisms = new File(importConfig.getString(CONFKEY_ORGANISMS));
		File ncbiTaxNames = new File(importConfig.getString(CONFKEY_ORGANISMS_NAMES));
		File homologene = new File(importConfig.getString(CONFKEY_HOMOLOGENE));
		File geneGroup = new File(importConfig.getString(CONFKEY_GENE_GROUP));
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
			Map<TermCoordinates, ImportConcept> termsByGeneId = new HashMap<>();
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

	@Override
	public boolean hasName(String providername) {
		return getClass().getCanonicalName().equals(providername) || "julielabgenes".equals(providername);
	}

}
