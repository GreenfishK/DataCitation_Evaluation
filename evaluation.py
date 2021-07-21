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
    assert query_type in ["simple", "complex"], "Query must be either simple or complex."
    if dataset_size == "small":
        citation = ct.Citation(config.get('GRAPHDB_RDFSTORE_FHIR', 'get'), config.get('GRAPHDB_RDFSTORE_FHIR', 'post'))
        post_endpoint = config.get('GRAPHDB_RDFSTORE_FHIR', 'post')
        query = open("FHIR/{0}_query.txt".format(query_type), "r").read()
        query_checksum = "{0}_query_fhir_checksum".format(query_type)
        delete_random_data = open("FHIR/delete_random_data.txt", "r").read()
    elif dataset_size == "big":
        citation = ct.Citation(config.get('GRAPHDB_RDFSTORE_WIKI', 'get'), config.get('GRAPHDB_RDFSTORE_WIKI', 'post'))
        post_endpoint = config.get('GRAPHDB_RDFSTORE_WIKI', 'post')
        query = open("Wikipedia/{0}_query.txt".format(query_type), "r").read()
        query_checksum = "{0}_query_wiki_checksum".format(query_type)
        delete_random_data = open("Wikipedia/delete_random_data.txt", "r").read()
    else:
        raise Exception("Dataset size must either be big or small.")
    rdf_engine = rdf.TripleStoreEngine(config.get('GRAPHDB_RDFSTORE_FHIR', 'get'),
                                       config.get('GRAPHDB_RDFSTORE_FHIR', 'post'))

    if versioning_mode == "mem_sav":
        vers_mode = VersioningMode.SAVE_MEM
    elif versioning_mode == "q_perf":
        vers_mode = VersioningMode.Q_PERF
    else:
        raise Exception("Please set the versioning mode either to mem_sav or q_perf.")
    if procedure_to_evaluate != 'init_versioning':
        rdf_engine.version_all_rows(versioning_mode=vers_mode)

    current_datetime = datetime.now()
    timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
    citation_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
    citation_timestamp = citation_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + citation_datetime.strftime("%z")[3:5]

    for i in range(1, 11):
        # Perform action and measure time and memory
        # TODO: procedures: 'init_versioning', 're-cite_query', 'retrieve_live_data', 'retrieve_history_data'
        procs = {'cite_query': [citation.cite, (query, metadata)],
                 'init_versioning': [rdf_engine.version_all_rows, vers_mode],
                 're-cite_query': [citation.cite, (query, metadata)],
                 'retrieve_live_data': [rdf_engine.get_data, query],
                 'retrieve_history_data': [rdf_engine.get_data, (query, citation_timestamp)]}
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

    # Reset experiment environment and settings
    update_triplestore(delete_random_data, post_endpoint)
    query_store._remove(config.get("QUERY", query_checksum))
    rdf_engine.reset_all_versions()

    return eval_results


evaluate(write_operation="insert", versioning_mode="mem_sav", procedure_to_evaluate="cite_query",
         query_type="simple", dataset_size="small")
evaluate(write_operation="insert", versioning_mode="mem_sav", procedure_to_evaluate="cite_query",
         query_type="complex", dataset_size="small")

# Save evaluation results to csv
logging.info("Saving evaluation results")
eval_results.to_csv("evaluation_results.csv")

# TODO: do 100 runs and take the average for each record in evaluation_results.csv
