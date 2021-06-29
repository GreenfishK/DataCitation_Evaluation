from rdf_data_citation import rdf_star as rdf
import configparser
import logging

# Load configs
config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))
# Load query
simple_query_fhir = open("FHIR/simple_query.txt", "r").read()

# GraphDB
logging.info("GraphDB evaluation")

# Setup RDF* API
rdf_engine = rdf.TripleStoreEngine(config.get('GRAPHDB_RDFSTORE_FHIR', 'get'),
                                   config.get('GRAPHDB_RDFSTORE_FHIR', 'post'))
# Execute query against RDF* store
try:
    result_set = rdf_engine.get_data(select_statement=simple_query_fhir, yn_timestamp_query=False)
    print(result_set.count())
except Exception as e:
    print(e)

# Stardog
"""
Run stardog on linux
Follow this instruction: https://www.stardog.com/blog/a-stardog-app-in-5-easy-steps/
Extract stardog to /opt/stardog instead of /data/stardog
export STARDOG_HOME=/opt/stardog
copy stardog-license-bin.bin into /opt/stardog/bin
execute following command from opt/stardog/bin:
     sudo ./stardog-admin server start

Sign up for https://www.stardog.com/studio/ with university e-mail address
Access stardog studio via https://stardog.studio/#/

Endpoints:
read: http://localhost:5820/DataCitation_FHIR/query
update: http://localhost:5820/DataCitation_FHIR/update

Failed experiment: Authentication does not seem to work with SPARQL Wrapper and Stardog
We get following message: Unauthorized: access is denied due to invalid credentials (unauthorized). 
Check the credentials.
"""
logging.info("Stardog evaluation")

# Setup RDF* API
credentials = rdf.TripleStoreEngine.Credentials('admin', 'admin')
rdf_engine_2 = rdf.TripleStoreEngine(config.get('STARDOG_RDFSTORE_FHIR', 'get'),
                                     config.get('STARDOG_RDFSTORE_FHIR', 'post'),
                                     credentials=credentials)
# Execute query against RDF* store
try:
    result_set_2 = rdf_engine_2.get_data(select_statement=simple_query_fhir, yn_timestamp_query=False, )
    print(result_set_2.count())
except Exception as e:
    print(e)

# Apache Jena
# TODO Query and write statement to Citation repository in Apache Jena using rdf_data_citation
