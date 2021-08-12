import subprocess
from SPARQLWrapper import SPARQLWrapper, DIGEST, POST, GET, JSON
from rdf_data_citation import query_handler as ct
from rdf_data_citation import persistent_id_utils as ct_ut
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
import gc

# Read config parameters such as query checksums of the evaluation queries and rdf store endpoints
config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))

# Setup evaluation results table
write_operations = ['timestamped_insert', 'timestamped_update']
dataset_sizes = ['small', 'big']
versioning_modes = ['q_perf', 'mem_sav']
query_types = ['simple_query', 'complex_query']
procedures_to_evaluate = ['cite_query', 're-cite_query', 'retrieve_live_data', 'retrieve_history_data']
increments = list(range(1, 11))
my_index_1 = pd.MultiIndex.from_product(iterables=[write_operations, dataset_sizes, versioning_modes,
                                                   query_types, procedures_to_evaluate, increments],
                                        names=['write_operation', 'dataset_size', 'versioning_mode',
                                               'query_type', 'procedure_to_evaluate', 'Increment'])
my_index_2 = pd.MultiIndex.from_product(iterables=[['insert'], dataset_sizes, versioning_modes,
                                                   ['no_query'], ['init_versioning'], increments],
                                        names=['write_operation', 'dataset_size', 'versioning_mode',
                                               'query_type', 'procedure_to_evaluate', 'Increment'])
my_index = my_index_1.union(my_index_2)
eval_results = pd.DataFrame(columns=['memory_in_MB', 'Memory_in_MB_instances', 'time_in_seconds', 'cnt_triples'],
                            index=my_index)

# init metadata
metadata = ct_ut.MetaData(identifier="simple_query_fhir_eval_20210602143900",
                          publisher="Filip Kovacevic",
                          resource_type="RDF",
                          publication_year="2021",
                          creator="Filip Kovacevic",
                          title="Simple FHIR query")

# current evaluation parameters
current_eval_params = {"procedures_to_evaluate": "",
                       "get_endpoint": "",
                       "post_endpoint": "",
                       "query_checksum": ""}


def update_triplestore(insert_statement: str, endpoint):
    sparql_post = SPARQLWrapper(endpoint)
    sparql_post.setHTTPAuth(DIGEST)
    sparql_post.setMethod(POST)
    sparql_post.setQuery(insert_statement)
    sparql_post.query()


def cnt_triples(endpoint) -> int:
    logging.info("Counting triples ...")
    query = "Select (count(*) as ?cnt) where {?s ?p ?o}"
    sparql_get = SPARQLWrapper(endpoint)
    sparql_get.setReturnFormat(JSON)
    sparql_get.setHTTPAuth(DIGEST)
    sparql_get.setMethod(GET)
    sparql_get.setQuery(query)
    query_result = sparql_get.query().convert()

    cnt = 0
    for entry in query_result['results']['bindings']:
        for sparql_variable in entry.keys():
            if sparql_variable == "cnt":
                cnt = int(entry[sparql_variable]['value'])
    logging.info("Currently there are {0} triples in the database".format(cnt))
    return cnt


def reset_experiment(procedure_to_evaluate: str, get_endpoint: str, post_endpoint: str, query_checksum: str):
    # Reset experiment environment and settings
    query_store = qs.QueryStore()
    rdf_engine = rdf.TripleStoreEngine(get_endpoint, post_endpoint)
    delete_random_data = open("delete_random_data.txt", "r").read()
    update_triplestore(delete_random_data, post_endpoint)
    if procedure_to_evaluate != "init_versioning":
        query_store._remove(config.get("QUERY", query_checksum))
    rdf_engine.reset_all_versions()
    # to prevent heap overflow
    gc.collect()


def evaluate(write_operation: str, dataset_size: str, versioning_mode: str, query_type: str,
             procedure_to_evaluate: str, output_file: str):
    # Evaluation
    logging.info("Start evaluation with parameters: {0}, {1}, {2}, {3}, {4}".format(write_operation, dataset_size,
                                                                                    versioning_mode, query_type,
                                                                                    procedure_to_evaluate))

    # Init parameters for evaluation
    assert query_type in ["simple_query", "complex_query", "no_query"], "Query must be either simple, complex or none. " \
                                                                    "Latter should only be used in case of init_versioning"

    if dataset_size == "small":
        get_endpoint = config.get('JENA_RDFSTORE_FHIR', 'get')
        post_endpoint = config.get('JENA_RDFSTORE_FHIR', 'post')
        if procedure_to_evaluate != "init_versioning":
            query = open("FHIR/{0}.txt".format(query_type), "r").read()
            query_checksum = "{0}_fhir_checksum".format(query_type)
        else:
            query = "no_query"
            query_checksum = None
        init_insert_random_data = open("FHIR/insert_random_data.txt", "r").read()
    elif dataset_size == "big":
        get_endpoint = config.get('JENA_RDFSTORE_WIKI', 'get')
        post_endpoint = config.get('JENA_RDFSTORE_WIKI', 'post')
        if procedure_to_evaluate != "init_versioning":
            query = open("Wikipedia/{0}.txt".format(query_type), "r").read()
            query_checksum = "{0}_wiki_checksum".format(query_type)
        else:
            query = "no_query"
            query_checksum = None
        init_insert_random_data = open("Wikipedia/insert_random_data.txt", "r").read()
    else:
        raise Exception("Dataset size must either be big or small.")

    current_eval_params['procedure_to_evaluate'] = procedure_to_evaluate
    current_eval_params['get_endpoint'] = get_endpoint
    current_eval_params['post_endpoint'] = post_endpoint
    current_eval_params['query_checksum'] = query_checksum

    # Capture memory of constructed objects
    tracemalloc.start()
    query_store = qs.QueryStore()
    rdf_engine = rdf.TripleStoreEngine(get_endpoint, post_endpoint)
    citation = ct.QueryHandler(get_endpoint, post_endpoint)
    mem_in_MB_instances = tracemalloc.get_traced_memory()[1] / 1024.0 / 1024.0  # peak memory
    tracemalloc.stop()
    # initial insert of random values labeled with the suffix _new_value. These are used as a starting point for
    # the update operation. These values are updated with new random values on each increment.
    update_triplestore(init_insert_random_data, post_endpoint)

    # Initial versioning
    vieTZObject = timezone(timedelta(hours=2))
    current_datetime = datetime.now(vieTZObject)
    if versioning_mode == "mem_sav":
        vers_mode = VersioningMode.SAVE_MEM
        init_timestamp = None
    elif versioning_mode == "q_perf":
        vers_mode = VersioningMode.Q_PERF
        init_timestamp = current_datetime
    else:
        raise Exception("Please set the versioning mode either to mem_sav or q_perf.")
    if procedure_to_evaluate != 'init_versioning':
        rdf_engine.version_all_rows(versioning_mode=vers_mode, initial_timestamp=init_timestamp)

    # TODO 20210812: Change cite_query --> mint_query_pid and re-cite_query --> re-execute_existing_query
    # Procedures to evaluate and parameters
    procs = {'cite_query': [citation.mint_query_pid, (query, metadata)],
             'init_versioning': [rdf_engine.version_all_rows, [vers_mode]],
             're-cite_query': [citation.mint_query_pid, (query, metadata)],
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
        tracemalloc.stop()

        # Insert or update new random triples with fixed dataset size (10% of initial superset)
        assert write_operation in ["insert", "timestamped_insert", "timestamped_update"], \
            "Write operation must be either insert, timestamped_insert or timestamped_update"
        if dataset_size == "small":
            insert_random_data = open("FHIR/{0}_random_data.txt".format(write_operation), "r").read()
        elif dataset_size == "big":
            insert_random_data = open("Wikipedia/{0}_random_data.txt".format(write_operation), "r").read()
        else:
            raise Exception("Dataset size must either be big or small.")
        cnt_trpls = cnt_triples(get_endpoint)
        update_triplestore(insert_random_data, post_endpoint)

        # Save evaluation results
        eval_results.loc[(write_operation, dataset_size, versioning_mode, query_type,
                           procedure_to_evaluate, i)] = [memMB, mem_in_MB_instances, time_elapsed, cnt_trpls]
        eval_results.to_csv(output_file, sep=";")
        """evaluation_results = open(output_file, "a")
        evaluation_results.write("{0};{1};{2};{3};{4};{5};{6};{7};{8};{9}\n".format(write_operation, dataset_size,
                                                                                versioning_mode, query_type,
                                                                                procedure_to_evaluate, i,
                                                                                memMB, time_elapsed,
                                                                                mem_in_MB_instances, cnt_trpls))
        evaluation_results.close()"""

        # Remove citation from query store
        if procedure_to_evaluate == "cite_query":
            query_store._remove(config.get("QUERY", query_checksum))
        elif procedure_to_evaluate == "init_versioning":
            rdf_engine.reset_all_versions()

    # Reset experiment environment and settings
    reset_experiment(procedure_to_evaluate, get_endpoint, post_endpoint, query_checksum)


"""
Apache Jena
# Load data via CLI
export FUSEKI_HOME=/opt/apache_jena_fuseki
export PATH=$PATH:/opt/apache_jena_fuseki 
export PATH_TO_DATA=~/Dokumente/Uni/Master/8._Semester/Master_thesis/Research/Evaluation/RDF\ Data\ Citation\ API/orig_data
fuseki-server
s-put http://localhost:3030/DataCitation_FHIR default $PATH_TO_DATA/fhir.ttl 
s-put http://localhost:3030/DataCitation_FHIR default $PATH_TO_DATA/rim.ttl 
s-put http://localhost:3030/DataCitation_FHIR default $PATH_TO_DATA/w5.ttl  

# Load data via GUI
# Load via GUI on http://localhost:3030/
"""


# Run evaluation 10 times
for i in range(10):
    logging.info("Starting run {0}".format(i))

    param_sets = set([set[:5] for set in my_index.tolist()])
    for c, param_set in enumerate(param_sets):
        logging.info("Scenario {0} starting".format(c))

        evaluate(*param_set, "evaluation_results_v20210811_{0}.csv".format(i))


# Use to run single scenarios which failed because of a heap overflow in GraphDB
# evaluate("timestamped_update", "big", "q_perf", "complex_query", "re-cite_query", "evaluation_results6.csv")
