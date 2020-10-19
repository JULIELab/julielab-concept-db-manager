package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.spi.FacetCreator;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;

import static de.julielab.concepts.db.core.ConfigurationConstants.CREATOR;
import static de.julielab.concepts.db.core.ConfigurationConstants.REQUEST;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class DefaultFacetCreator implements FacetCreator {

	public static final String FACET_GROUP_NAME = slash("facetgroup", "name");
	public static final String NAME = "name";
	public static final String SHORT_NAME = "shortname";
	public static final String CUSTOM_ID = "customid";
	public static final String SOURCE_TYPE = "sourcetype";
	public static final String LABELS = "labels";
	public static final String NO_FACET = "nofacet";

	@Override
	public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> facetConfiguration, Object facetData)
			throws FacetCreationException {
		String configPath = slash(CREATOR, REQUEST);
		String facetGroupName = facetConfiguration.getString(slash(configPath, FACET_GROUP_NAME), "Default Facet Group");
		String name = facetConfiguration.getString(slash(configPath, NAME), "Default Facet");
		String shortName = facetConfiguration.getString(slash(configPath, SHORT_NAME), "Default");
		String customId = facetConfiguration.getString(slash(configPath, CUSTOM_ID), "defaultfacetid");
		String sourceType = facetConfiguration.getString(slash(configPath, SOURCE_TYPE), FacetConstants.SRC_TYPE_HIERARCHICAL);
		List<String> labels = facetConfiguration.getList(String.class, slash(configPath, LABELS));
		boolean noFacet = facetConfiguration.getBoolean(slash(configPath, NO_FACET), false);

		ImportFacetGroup fg = new ImportFacetGroup(facetGroupName);
		return new ImportFacet(fg, customId, name, shortName, sourceType, labels, noFacet);
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}

}
