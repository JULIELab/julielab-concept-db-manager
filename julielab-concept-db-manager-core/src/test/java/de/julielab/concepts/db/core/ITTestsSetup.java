package de.julielab.concepts.db.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(suiteName = "integration-tests")
public class ITTestsSetup {
    private final static Logger log = LoggerFactory.getLogger(ITTestsSetup.class);

    public static GenericContainer neo4j = new GenericContainer("neo4j:3.5.17").
            withEnv("NEO4J_AUTH", "none").withExposedPorts(7474, 7687).
            withClasspathResourceMapping("julielab-neo4j-plugins-concepts-2.1.0-SNAPSHOT-assembly.jar",
                    "/var/lib/neo4j/plugins/julielab-neo4j-plugins-concepts-2.1.0-SNAPSHOT-assembly.jar",
                    BindMode.READ_WRITE);

    @BeforeSuite(groups = "integration-tests")
    public static void setupSuite() {
        neo4j.start();
        Slf4jLogConsumer toStringConsumer = new Slf4jLogConsumer(log);
        neo4j.followOutput(toStringConsumer, OutputFrame.OutputType.STDOUT);
    }

    @AfterSuite(groups = "integration-tests")
    public static void shutdownSuite() {
        neo4j.stop();
    }
}
