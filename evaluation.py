from SPARQLWrapper import SPARQLWrapper, DIGEST, POST
from rdf_data_citation import citation as ct
from rdf_data_citation import citation_utils as ct_ut
from rdf_data_citation import rdf_star as rdf
from rdf_data_citation.rdf_star import VersioningMode
from rdf_data_citation import query_store as qs
import configparser
import logging
import time
import pandas as pd
import tracemalloc
from datetime import datetime, timedelta, timezone
import tzlocal

config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))

# Setup evaluation results table
write_operations = ['insert', 'update']
dataset_sizes = ['small', 'big']
versioning_modes = ['q_perf', 'mem_sav']
query_types = ['simple_query', 'complex_query']
procedures_to_evaluate = ['init_versioning', 'cite_query', 're-cite_query', 'retrieve_live_data', 'retrieve_history_data']
rounds = list(range(1, 11))
my_index = pd.MultiIndex.from_product(iterables=[rounds, write_operations, dataset_sizes, versioning_modes,
                                                 query_types, procedures_to_evaluate],
                                      names=['round', 'write_operation', 'dataset', 'versioning_mode', 'query_type',
                                             'procedure_to_evaluate'])
eval_results = pd.DataFrame(columns=['memory_in_MB', 'time_in_seconds'],
                            index=my_index)

# init metadata
metadata = ct_ut.MetaData(identifier="simple_query_fhir_eval_20210602143900",
                          publisher="Filip Kovacevic",
                          resource_type="RDF",
                          publication_year="2021",
                          creator="Filip Kovacevic",
                          title="Simple FHIR query")


def update_triplestore(insert_statement: str, endpoint):
    sparql_post = SPARQLWrapper(endpoint)
    sparql_post.setHTTPAuth(DIGEST)
    sparql_post.setMethod(POST)
    sparql_post.setQuery(insert_statement)
    sparql_post.query()


def evaluate(write_operation: str, dataset_size: str, versioning_mode: str, query_type: str, procedure_to_evaluate: str):
    # Evaluation
    logging.info("Start evaluation with parameters: insert, small, mem_sav, simple query, cite_query")

    # Init parameters for evaluation
    query_store = qs.QueryStore()
    assert query_type in ["simple", "complex", "none"], "Query must be either simple, complex or none. " \
                                                        "Latter should only be used in case of init_versioning"

    if dataset_size == "small":
        get_endpoint = config.get('GRAPHDB_RDFSTORE_FHIR', 'get')
        post_endpoint = config.get('GRAPHDB_RDFSTORE_FHIR', 'post')
        if procedure_to_evaluate != "init_versioning":
            query = open("FHIR/{0}_query.txt".format(query_type), "r").read()
            query_checksum = "{0}_query_fhir_checksum".format(query_type)
        else:
            query = None
            query_checksum = None
        delete_random_data = open("FHIR/delete_random_data.txt", "r").read()
    elif dataset_size == "big":
        get_endpoint = config.get('GRAPHDB_RDFSTORE_WIKI', 'get')
        post_endpoint = config.get('GRAPHDB_RDFSTORE_WIKI', 'post')
        if procedure_to_evaluate != "init_versioning":
            query = open("Wikipedia/{0}_query.txt".format(query_type), "r").read()
            query_checksum = "{0}_query_wiki_checksum".format(query_type)
        else:
            query = None
            query_checksum = None
        delete_random_data = open("Wikipedia/delete_random_data.txt", "r").read()
    else:
        raise Exception("Dataset size must either be big or small.")
    rdf_engine = rdf.TripleStoreEngine(get_endpoint, post_endpoint)
    citation = ct.Citation(get_endpoint, post_endpoint)

    if versioning_mode == "mem_sav":
        vers_mode = VersioningMode.SAVE_MEM
    elif versioning_mode == "q_perf":
        vers_mode = VersioningMode.Q_PERF
    else:
        raise Exception("Please set the versioning mode either to mem_sav or q_perf.")
    if procedure_to_evaluate != 'init_versioning':
        rdf_engine.version_all_rows(versioning_mode=vers_mode)
    vieTZObject = timezone(timedelta(hours=2))
    current_datetime = datetime.now(vieTZObject)

    procs = {'cite_query': [citation.cite, (query, metadata)],
             'init_versioning': [rdf_engine.version_all_rows, [vers_mode]],
             're-cite_query': [citation.cite, (query, metadata)],
             'retrieve_live_data': [rdf_engine.get_data, [query]],
             'retrieve_history_data': [rdf_engine.get_data, (query, current_datetime)]}

    for i in range(1, 11):
        # Perform action and measure time and memory

        time_start = time.perf_counter()
        tracemalloc.start()
        ################################################################################################################
        func = procs[procedure_to_evaluate][0]
        func_params = procs[procedure_to_evaluate][1]
        func(*func_params)
        ################################################################################################################
        time_elapsed = (time.perf_counter() - time_start)
        memMB = tracemalloc.get_traced_memory()[1] / 1024.0 / 1024.0  # peak memory
        print(citation.query_utils.checksum)
        # memMB = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0 / 1024.0

        # Insert or update new random triples with fixed dataset size (10% of initial superset)
        assert write_operation in ["insert", "update"], "Write operation must be either insert or update"
        if dataset_size == "small":
            if procedure_to_evaluate == "init_versioning":
                insert_random_data = open("FHIR/{0}_random_data.txt".format(write_operation), "r").read()
            else:
                insert_random_data = open("FHIR/{0}_timestamped_random_data.txt".format(write_operation), "r").read()
        elif dataset_size == "big":
            if procedure_to_evaluate == "init_versioning":
                insert_random_data = open("Wikipedia/{0}_random_data.txt".format(write_operation), "r").read()
            else:
                insert_random_data = open("Wikipedia/{0}_timestamped_random_data.txt".format(write_operation), "r").read()
        else:
            raise Exception("Dataset size must either be big or small.")
        update_triplestore(insert_random_data, post_endpoint)

        # Save evaluation results
        eval_results.loc[(i, write_operation, dataset_size, versioning_mode, query_type + "_query",
                          procedure_to_evaluate)] = [memMB, time_elapsed]

        # Remove citation from query store
        if procedure_to_evaluate == "cite_query":
            query_store._remove(config.get("QUERY", query_checksum))
        elif procedure_to_evaluate == "init_versioning":
            rdf_engine.reset_all_versions()

    # Reset experiment environment and settings
    update_triplestore(delete_random_data, post_endpoint)
    if procedure_to_evaluate == "cite_query":
        query_store._remove(config.get("QUERY", query_checksum))
    rdf_engine.reset_all_versions()

    return eval_results


param_sets = [("insert", "small", "mem_sav", "simple", "cite_query")]
for param_set in param_sets:
    evaluate(*param_set)

# query_store = qs.QueryStore()
# query_store._remove(config.get("QUERY", "simple_query_fhir_checksum"))
# query_store._remove(config.get("QUERY", "complex_query_fhir_checksum"))

# Save evaluation results to csv
logging.info("Saving evaluation results")
eval_results.to_csv("evaluation_results.csv")

# TODO: do 10 runs and take the average for each record in evaluation_results.csv
# TODO: create list of parameters for evaluate() and run them all
