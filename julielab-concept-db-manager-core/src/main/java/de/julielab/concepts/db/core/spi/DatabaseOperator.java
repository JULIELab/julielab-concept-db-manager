package de.julielab.concepts.db.core.spi;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface DatabaseOperator extends ExtensionPoint, DatabaseConnected {
    void operate(HierarchicalConfiguration<ImmutableNode> operationConfigration);

}
