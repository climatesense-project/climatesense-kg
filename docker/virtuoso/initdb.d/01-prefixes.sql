--
-- Add prefixes
--

DB.DBA.XML_SET_NS_DECL ('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 2);
DB.DBA.XML_SET_NS_DECL ('rdfs', 'http://www.w3.org/2000/01/rdf-schema#', 2);
DB.DBA.XML_SET_NS_DECL ('owl', 'http://www.w3.org/2002/07/owl#', 2);
DB.DBA.XML_SET_NS_DECL ('xsd', 'http://www.w3.org/2001/XMLSchema#', 2);
DB.DBA.XML_SET_NS_DECL ('dc', 'http://purl.org/dc/elements/1.1/', 2);
DB.DBA.XML_SET_NS_DECL ('schema', 'http://schema.org/', 2);
DB.DBA.XML_SET_NS_DECL ('skos', 'http://www.w3.org/2004/02/skos/core#', 2);
DB.DBA.XML_SET_NS_DECL ('cimple', 'http://data.cimple.eu/ontology#', 2);
DB.DBA.XML_SET_NS_DECL ('climatesense', 'http://data.climatesense-project.eu/ontology#', 2);

--
--  End of script
--
