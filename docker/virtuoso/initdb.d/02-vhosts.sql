--
-- Add vhost entries
--

--
-- /claim-review
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/claim-review');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/claim-review', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_1'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_1', 1, vector ('http_rule_1'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_1', 1, '/claim-review/(.*)', vector ('par_1'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/claim-review/%U', vector ('par_1'), NULL, NULL, 2, 0, '');

--
-- /claim
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/claim');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/claim', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_2'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_2', 1, vector ('http_rule_2'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_2', 1, '/claim/(.*)', vector ('par_2'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/claim/%U', vector ('par_2'), NULL, NULL, 2, 0, '');

--
-- /organization
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/organization');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/organization', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_3'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_3', 1, vector ('http_rule_3'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_3', 1, '/organization/(.*)', vector ('par_3'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/organization/%U', vector ('par_3'), NULL, NULL, 2, 0, '');

--
-- /rating
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/rating');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/rating', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_4'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_4', 1, vector ('http_rule_4'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_4', 1, '/rating/(.*)', vector ('par_4'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/rating/%U', vector ('par_4'), NULL, NULL, 2, 0, '');

--
-- /emotion
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/emotion');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/emotion', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_5'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_5', 1, vector ('http_rule_5'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_5', 1, '/emotion/(.*)', vector ('par_5'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/emotion/%U', vector ('par_5'), NULL, NULL, 2, 0, '');

--
-- /political-leaning
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/political-leaning');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/political-leaning', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_6'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_6', 1, vector ('http_rule_6'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_6', 1, '/political-leaning/(.*)', vector ('par_6'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/political-leaning/%U', vector ('par_6'), NULL, NULL, 2, 0, '');

--
-- /conspiracy
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/conspiracy');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/conspiracy', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_7'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_7', 1, vector ('http_rule_7'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_7', 1, '/conspiracy/(.*)', vector ('par_7'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/conspiracy/%U', vector ('par_7'), NULL, NULL, 2, 0, '');

--
-- /graph
--
DB.DBA.VHOST_REMOVE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/graph');
DB.DBA.VHOST_DEFINE (lhost=>'*ini*', vhost=>'*ini*', lpath=>'/graph', ppath=>'/DAV/', is_dav=>1, is_brws=>0, def_page=>'', vsp_user=>'dba', ses_vars=>0, opts=>vector ('browse_sheet', '', 'cors', '*', 'cors_restricted', 0, 'url_rewrite', 'http_rule_list_8'), is_default_host=>0);
DB.DBA.URLREWRITE_CREATE_RULELIST ('http_rule_list_8', 1, vector ('http_rule_8'));
DB.DBA.URLREWRITE_CREATE_REGEX_RULE ('http_rule_8', 1, '/graph/(.*)', vector ('par_8'), 1, '/fct/rdfdesc/description.vsp?g=http://data.climatesense-project.eu/graph/%U', vector ('par_8'), NULL, NULL, 2, 0, '');

--
--  End of script
--
