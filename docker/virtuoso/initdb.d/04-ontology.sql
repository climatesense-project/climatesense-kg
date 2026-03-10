--
-- Load the vocabularies into a dedicated named graph
--

delete from DB.DBA.LOAD_LIST;
ld_dir('/database/data/vocabularies', '*.ttl', 'http://data.climatesense-project.eu/graph/vocabularies');
rdf_loader_run();
checkpoint;

--
-- End of script
--
