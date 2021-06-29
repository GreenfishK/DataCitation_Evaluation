from rdf_data_citation import rdf_star as rdf
import configparser
import logging

# Load configs
config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))
# Load query
simple_query_fhir = open("FHIR/simple_query.txt", "r").read()


def eval_graphdb():
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


def eval_stardog():
    # Stardog
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


def eval_jena():
    # Apache Jena
    logging.info("Apache Jena evaluation")

    # Setup RDF* API
    rdf_engine = rdf.TripleStoreEngine(config.get('JENA_RDFSTORE_FHIR', 'get'),
                                       config.get('JENA_RDFSTORE_FHIR', 'post'))
    # Execute query against RDF* store
    try:
        result_set = rdf_engine.get_data(select_statement=simple_query_fhir, yn_timestamp_query=False)
        print(result_set.count())
    except Exception as e:
        print(e)

#eval_graphdb()
#eval_stardog()
eval_jena()

