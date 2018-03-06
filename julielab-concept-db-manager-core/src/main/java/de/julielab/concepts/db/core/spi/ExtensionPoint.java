package de.julielab.concepts.db.core.spi;

/**
 * <p>
 * A very simple interface that indicates service provider extension points. The idea is that some service provider
 * interfaces are extension points in the sense that there might be custom implementation of the interface that
 * are lookup up by the {@link java.util.ServiceLoader} used by the respective service. Implementations are
 * identified by a name. This interface defines {@link #hasName(String)} to check whether a service provider matches
 * the sought name or not.
 * </p>
 * <p>The contract is that service providers must always accept their qualified class name.</p>
 */
public interface ExtensionPoint {
    /**
     * Checks if a concrete service provider implementation of this service provider
     * interface matches the given name. The naming contract enforces that each
     * provider must accept its own qualified class name as provider name. Other
     * names are optional and might facilitate the usage of the provider in the
     * configuration file.
     *
     * @param providername
     *            A name to check whether it matches the name of a provider.
     * @return True, if the given name matches the provider's, false otherwise.
     */
    boolean hasName(String providername);
}
