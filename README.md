A fork of existing repository https://github.com/improvedk/OrcaMDF with the following enhancements:

 - modern data types support (Xml, HierarchyId, Geography, Geometry, DateTime2, DateTimeOffset);
 - improved formatting of the data shown in grid;
 - caching system tables only;
 - large databases support (tested on the Stack Overflow 2014 database https://www.brentozar.com/archive/2015/10/how-to-download-the-stack-overflow-database-via-bittorrent/);
 - schema added to names of the tables and data types;
 - fixed some bugs (like usage of pg_first - it works mainly for the sample DBs, but doesn't for real DBs, bug with multiple bit columns - https://github.com/improvedk/OrcaMDF/issues/32 etc.);
 - ability to generate an SQL script to fix corrupted page of database based on the page data from the file;
 - ability to export the data of the table from the backup to the SQL Server for further analysis;
 - columns with zero physical length (with default constraints);
 - skipping dropped column data;
 - skipping ghost ang ghost forwarded records;
 - external plugins support;
 - collation support;
 - SQL syntax highlighting;
 - performance optimization.
