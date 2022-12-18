# Changelog

## v1.3.0 (18/12/2022)
- [**enhancement**] Add GO annotations [#17](https://github.com/JULIELab/julielab-concept-db-manager/issues/17)
- [**enhancement**] Add FamPlex concept import. [#15](https://github.com/JULIELab/julielab-concept-db-manager/issues/15)
- [**closed**] Support javax.ws.rs.StreamOutput [#13](https://github.com/JULIELab/julielab-concept-db-manager/issues/13)
- [**closed**] Bump dependency versions [#12](https://github.com/JULIELab/julielab-concept-db-manager/issues/12)

---

## v1.1.1 (10/01/2022)
Bug fixes.
---

## v1.1.0 (19/10/2020)
- [**closed**] Stream HTTP responses [#10](https://github.com/JULIELab/julielab-concept-db-manager/issues/10)
- [**closed**] NCBI Gene Concept creator: Remove homologene. [#9](https://github.com/JULIELab/julielab-concept-db-manager/issues/9)
- [**closed**] Let NCBI Gene Concept Creator stream from disc [#8](https://github.com/JULIELab/julielab-concept-db-manager/issues/8)
- [**closed**] Add the total number of concepts to `ImportConcepts` [#7](https://github.com/JULIELab/julielab-concept-db-manager/issues/7)
- [**closed**] Try to unify import/operate/export configuration XML schemas [#6](https://github.com/JULIELab/julielab-concept-db-manager/issues/6)
- [**closed**] Stream concepts to server [#5](https://github.com/JULIELab/julielab-concept-db-manager/issues/5)
- [**closed**] Update Neo4j to 4.0.4 [#4](https://github.com/JULIELab/julielab-concept-db-manager/issues/4)
- [**closed**] Support unmanaged server extensions [#3](https://github.com/JULIELab/julielab-concept-db-manager/issues/3)

---

## v1.0.0 (18/05/2020)
This version is nearly feature-complete with respect to the original requirements:
* Import concepts
* allow operations in the database
* export data
* realize database versioning
* support Cypher over HTTP and BOLT
* support Neo4j server plugins (removed in Neo4j 4.x).


---

## v0.0.1 (23/12/2017)
This is a very first working version of the JULIE Lab Concept Database Manager.
The current tool allows the following:

- Import concepts into Neo4j using the [julielab-neo4j-server-plugins-concept](https://github.com/JULIELab/julielab-neo4j-server-plugins/tree/master/julielab-neo4j-plugins-concepts) server plugins. This can be done via a HTTP connection or directly in an embedded database (which then can be used within a server!)
- Setting a database version on a node with the unique VERSION label. This help with resource management in your project. This works with any HTTP, BOLT or file database connection.
- Exporting data via ServerPlugins that return a string encoding a JSON array or a base64-encoded byte[], both of which may also be in GZIP format. Alternatively, an arbitrary Java class may be used that exposes a method taking the `GraphDatabaseService` as a first parameter and a configurable set of other parameters. The return value must always be a string as described above. See the jUnit tests in the exporter projects for more details.

The julielab-concept-db-application is a good starting point to actually use this functionality. The whole project bases on a single XML configuration file. The application takes this file and the task to perform (import, export, versioning) and then does it.

The XML configuration file format is currently only documented in two ways:

1. The `conceptdatabase.xsd` XML Schema file in `julielab-concept-db-manager-core/src/main/resources`.
2. The unit test resources in the core and other projects which use configuration files.

