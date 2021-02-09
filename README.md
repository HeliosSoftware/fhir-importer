Build using maven and run the jar or run the main class from an IDE.

Usage: `java -jar fhir-importer-0.0.1-SNAPSHOT.jar -directory <dir with FHIR resources> -server <FHIR endpoint url> -regex ".*.json"`

The importer can now be run against a directory with resource files in it (like the `/fhir/output` folder of synthea) or against a directory with nested Resource directories within it (like the output of the MITRE PDEX tool).

It assumes if it is run against a directory with the files directly in it that they are all bundles and if it is run against a nested directory, the Resource types match the name of the containing folder.
