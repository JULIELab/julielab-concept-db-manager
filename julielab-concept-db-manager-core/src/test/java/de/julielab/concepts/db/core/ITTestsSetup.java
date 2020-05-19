package de.julielab.concepts.db.core;

import de.julielab.neo4j.plugins.Indexes;
import de.julielab.neo4j.plugins.concepts.ConceptManager;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Test(suiteName = "integration-tests")
public class ITTestsSetup {
    private final static Logger log = LoggerFactory.getLogger(ITTestsSetup.class);

//    public static GenericContainer neo4j = new GenericContainer("neo4j:4.0.4").
//            withEnv("NEO4J_AUTH", "none").withExposedPorts(7474, 7687).
//            withClasspathResourceMapping("dockercontext/julielab-neo4j-plugins-concepts-3.0.0-SNAPSHOT-assembly.jar",
//                    "/var/lib/neo4j/plugins/julielab-neo4j-plugins-concepts-3.0.0-SNAPSHOT-assembly.jar",
//                    BindMode.READ_WRITE)
//            .
//            withClasspathResourceMapping("dockercontext/neo4j_docker.conf",
//                    "/var/lib/neo4j/conf/neo4j.conf2",
//                    BindMode.READ_WRITE)
//            ;

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

        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + neo4j.getMappedPort(7474) + "/concepts/"  + Indexes.INDEXES_REST_ENDPOINT+"/"+Indexes.CREATE_INDEXES+"?"+Indexes.DB_NAME+"="+DEFAULT_DATABASE_NAME).toURL().openConnection();
        conn.setRequestMethod("PUT");
        if(conn.getErrorStream() != null)
            log.error("Error occurred when trying to create indexes: {}", IOUtils.toString(conn.getErrorStream(), UTF_8));
    }

    @AfterSuite(groups = "integration-tests")
    public static void shutdownSuite() {
        neo4j.stop();
    }
}
