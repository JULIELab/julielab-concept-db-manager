package de.julielab.concepts.db.core;

import de.julielab.neo4j.plugins.Indexes;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.net.HttpURLConnection;
import java.net.URI;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Test(suiteName = "integration-tests")
public class ITTestsSetup {
    private final static Logger log = LoggerFactory.getLogger(ITTestsSetup.class);

    public static GenericContainer neo4j = new GenericContainer(
            new ImageFromDockerfile("cdbm-1.1.0-test", true)
                    .withFileFromClasspath("Dockerfile", "dockercontext/Dockerfile")
                    .withFileFromClasspath("julielab-neo4j-plugins-concepts-3.0.0-SNAPSHOT-assembly.jar", "dockercontext/julielab-neo4j-plugins-concepts-3.0.0-SNAPSHOT-assembly.jar")
                    .withFileFromClasspath("neo4j.conf", "dockercontext/neo4j.conf"))
            .withExposedPorts(7474, 7687)
            .withEnv("NEO4J_AUTH", "none");

    @BeforeSuite(groups = "integration-tests")
    public static void setupSuite() throws Exception {
        neo4j.start();
        Slf4jLogConsumer toStringConsumer = new Slf4jLogConsumer(log);
        neo4j.followOutput(toStringConsumer, OutputFrame.OutputType.STDOUT);

        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + neo4j.getMappedPort(7474) + "/concepts/" + Indexes.INDEXES_REST_ENDPOINT + "/" + Indexes.CREATE_INDEXES + "?" + Indexes.DB_NAME + "=" + DEFAULT_DATABASE_NAME).toURL().openConnection();
        conn.setRequestMethod("PUT");
        if (conn.getErrorStream() != null) {
            String errormsg = IOUtils.toString(conn.getErrorStream(), UTF_8);
            log.error("Error occurred when trying to create indexes: {}", errormsg);
            throw new IllegalArgumentException(errormsg);
        }
        // The actual response string is ignored on purpose. The index creation does not give a response. Still,
        // the input stream must be consumed.
        log.info("Successfully created concept indexes.", IOUtils.toString(conn.getInputStream(), UTF_8));
    }

    @AfterSuite(groups = "integration-tests")
    public static void shutdownSuite() {
        neo4j.stop();
    }
}
