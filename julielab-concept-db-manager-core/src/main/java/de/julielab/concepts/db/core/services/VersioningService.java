package de.julielab.concepts.db.core.services;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.spi.Versioning;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersioningException;

public class VersioningService {
	
	
	private static final Logger log = LoggerFactory.getLogger(VersioningService.class);
	
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
	private ServiceLoader<Versioning> loader;
	private static Map<HierarchicalConfiguration<ImmutableNode>, VersioningService> serviceMap;

	private VersioningService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		this.connectionConfiguration = connectionConfiguration;
		loader = ServiceLoader.load(Versioning.class);
	}

	public static synchronized VersioningService getInstance(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		if (serviceMap == null)
			serviceMap = new HashMap<>();
		return serviceMap.computeIfAbsent(connectionConfiguration, VersioningService::new);
	}
	
	public void setVersion(String version) throws VersioningException {
		Iterator<Versioning> iterator = loader.iterator();
		while (iterator.hasNext()) {
			Versioning versioning = iterator.next();
			try {
				versioning.setConnection(connectionConfiguration);
				versioning.setVersion(version);
			} catch (ConceptDatabaseConnectionException e) {
				log.debug("Versioning " + versioning.getClass().getCanonicalName() + " could not serve connection " + ConfigurationUtils.toString(connectionConfiguration));
			}
		}
	}
}
