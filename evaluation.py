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
    logging.info("Start evaluation with parameters: {0}, {1}, {2}, {3}, {4}".format(write_operation, dataset_size,
                                                                                    versioning_mode, query_type,
                                                                                    procedure_to_evaluate))

    # Init parameters for evaluation
    query_store = qs.QueryStore()
    assert query_type in ["simple_query", "complex_query", "no_query"], "Query must be either simple, complex or none. " \
                                                                    "Latter should only be used in case of init_versioning"

    if dataset_size == "small":
        get_endpoint = config.get('GRAPHDB_RDFSTORE_FHIR', 'get')
        post_endpoint = config.get('GRAPHDB_RDFSTORE_FHIR', 'post')
        if procedure_to_evaluate != "init_versioning":
            query = open("FHIR/{0}.txt".format(query_type), "r").read()
            query_checksum = "{0}_fhir_checksum".format(query_type)
        else:
            query = "no_query"
            query_checksum = None
        init_insert_random_data = open("FHIR/insert_random_data.txt", "r").read()
        delete_random_data = open("FHIR/delete_random_data.txt", "r").read()
    elif dataset_size == "big":
        get_endpoint = config.get('GRAPHDB_RDFSTORE_WIKI', 'get')
        post_endpoint = config.get('GRAPHDB_RDFSTORE_WIKI', 'post')
        if procedure_to_evaluate != "init_versioning":
            query = open("Wikipedia/{0}.txt".format(query_type), "r").read()
            query_checksum = "{0}_wiki_checksum".format(query_type)
        else:
            query = "no_query"
            query_checksum = None
        init_insert_random_data = open("Wikipedia/insert_random_data.txt", "r").read()
        delete_random_data = open("Wikipedia/delete_random_data.txt", "r").read()
    else:
        raise Exception("Dataset size must either be big or small.")
    rdf_engine = rdf.TripleStoreEngine(get_endpoint, post_endpoint)
    citation = ct.Citation(get_endpoint, post_endpoint)

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

    # Procedures to evaluate and parameters
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
        # memMB = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0 / 1024.0

        # Insert or update new random triples with fixed dataset size (10% of initial superset)
        assert write_operation in ["insert", "timestamped_insert", "timestamped_update"], \
            "Write operation must be either insert, timestamped_insert or timestamped_update"
        if dataset_size == "small":
            insert_random_data = open("FHIR/{0}_random_data.txt".format(write_operation), "r").read()
        elif dataset_size == "big":
            insert_random_data = open("Wikipedia/{0}_random_data.txt".format(write_operation), "r").read()
        else:
            raise Exception("Dataset size must either be big or small.")
        update_triplestore(insert_random_data, post_endpoint)

        # Save evaluation results
        eval_results.loc[(write_operation, dataset_size, versioning_mode, query_type,
                          procedure_to_evaluate, i)] = [memMB, time_elapsed]

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


param_sets = set([set[:5] for set in my_index.tolist()])

# init_versioning: none, dataset_size, versioning_modes
for c, param_set in enumerate(param_sets):
    print("Scenario {0} starting".format(c))
    evaluate(*param_set)

# Save evaluation results to csv
logging.info("Saving evaluation results")
eval_results.to_csv("evaluation_results.csv")

# TODO: do 10 runs and take the average for each record in evaluation_results.csv

