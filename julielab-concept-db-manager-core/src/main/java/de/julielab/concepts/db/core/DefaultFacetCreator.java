package de.julielab.concepts.db.core;

import java.util.List;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;

import de.julielab.concepts.db.core.spi.FacetCreator;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

public class DefaultFacetCreator implements FacetCreator {

	public static final String PROVIDER_NAME = "default";

	public static final String FACET_GROUP_NAME = "facetgroup.name";
	public static final String NAME = "name";
	public static final String SHORT_NAME = "shortname";
	public static final String CUSTOM_ID = "customid";
	public static final String SOURCE_TYPE = "sourcetype";
	public static final String LABELS = "labels";
	public static final String NO_FACET = "nofacet";

	@Override
	public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> facetConfiguration, Object facetData)
			throws FacetCreationException {
		String facetGroupName = facetConfiguration.getString(FACET_GROUP_NAME, "Default Facet Group");
		String name = facetConfiguration.getString(NAME, "Default Facet");
		String shortName = facetConfiguration.getString(SHORT_NAME, "Default");
		String customId = facetConfiguration.getString(CUSTOM_ID, "defaultfacetid");
		String sourceType = facetConfiguration.getString(SOURCE_TYPE, FacetConstants.SRC_TYPE_HIERARCHICAL);
		List<String> labels = facetConfiguration.getList(String.class, LABELS);
		boolean noFacet = facetConfiguration.getBoolean(NO_FACET, false);

		ImportFacetGroup fg = new ImportFacetGroup(facetGroupName);
		return new ImportFacet(fg, customId, name, shortName, sourceType, labels, noFacet);
	}

	@Override
	public boolean hasName(String providername) {
		// The default facet creator is able to create the "default" facet when no creator is given.
		return StringUtils.isBlank(providername) || providername.equals(getClass().getCanonicalName()) || providername.equals(PROVIDER_NAME);
	}

}
