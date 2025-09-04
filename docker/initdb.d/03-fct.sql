--
-- Set up Virtuoso Faceted Browser
--

--
-- Load the Faceted Browser VAD
--
vad_install('/opt/virtuoso-opensource/vad/fct_dav.vad', 0);

--
-- Add RDF full-text index and rules
--
RDF_OBJ_FT_RULE_ADD (null, null, 'All');
VT_INC_INDEX_DB_DBA_RDF_OBJ ();

--
-- Populate label lookup table and calculate IRI ranks
--
urilbl_ac_init_db()
s_rank()

--
--  End of script
--