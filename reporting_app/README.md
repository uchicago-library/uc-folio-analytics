# Reporting App queries
Queries for the FOLIO reporting app belong in this folder. 

Note: queries need both the .sql and .json files:
* the SQL file contains the query itself, and
* the JSON file defines the Web form and the parameter names used in the SQL query.

An optional Markdown (.md) file can provide documentation for the query.

# Schemata

Every SQL query file creates a function in a schema, and that schema is hard-coded in the file. 
This could be one schema where all queries go, or the queries may be distributed across more than one schemata.

## Resources
- [Authoring reports for the FOLIO Reporting App](https://github.com/folio-org/ui-ldp/blob/master/doc/reports.md) (documentation in the `ui-ldp` repo)
- [20250506 Reporting App for FOLIO Implementers](https://docs.google.com/presentation/d/1tnoxrlH5rGbLJS6Rg1hNoDcPDokIB9kCBDxgPHJ4Kds/) (Slide deck, start at slide 20, Using the Reporting App in FOLIO to Build and Run Real-Time Data reports)
