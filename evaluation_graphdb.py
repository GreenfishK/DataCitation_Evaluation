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
import pexpect

# Read config parameters such as query checksums of the evaluation queries and rdf store endpoints
config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))

# Setup evaluation results table
write_operations = ['timestamped_insert', 'timestamped_update']
dataset_sizes = ['small', 'big']
versioning_modes = ['q_perf', 'mem_sav']
query_types = ['simple_query', 'complex_query']
procedures_to_evaluate = ['mint_query_pid', 're-execute_query', 'retrieve_live_data', 'retrieve_history_data']
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


def delete_repos(repo_id: str):
    logging.info("Deleting repo {0}".format(repo_id))
    if repo_id.startswith('DataCitation_FHIR'):
        # Delete FHIR
        logging.info("Deleting FHIR repository")
        process1 = pexpect.spawnu(config.get("GRAPHDB", "sesame_console_path"))
        process1.timeout = 25
        process1.sendline("drop {0}".format(repo_id))
        process1.sendline("yes")
        process1.expect("Dropped repository '{0}'".format(repo_id))
        process1.close()

    if repo_id.startswith('DataCitation_CategoryLabels'):
        # Delete Wiki
        logging.info("Deleting Wiki repository")
        process2 = pexpect.spawnu(config.get("GRAPHDB", "sesame_console_path"))
        process2.timeout = 40
        process2.sendline("drop {0}".format(repo_id))
        process2.sendline("yes")
        process2.expect("Dropped repository '{0}'".format(repo_id))
        process2.close()


def create_repos_with_data(repo_id: str):
    if repo_id.startswith('DataCitation_FHIR'):
        # Create FHIR
        logging.info("Creating FHIR repository")
        create1 = pexpect.spawnu(config.get("GRAPHDB", "sesame_console_path"))
        create1.timeout = 300
        create1.sendline("create free")
        create1.sendline("{0}".format(repo_id))
        create1.sendline("Repository for Evaluation of the RDF Data Citation API")
        for k in range(18):
            create1.sendline("")
        create1.sendline("yes")
        create1.expect("Repository created")
        create1.close()

        # Load FHIR
        logging.info("Loading data into FHIR repository")
        fhir_data_path = config.get("GRAPHDB_RDFSTORE_FHIR", "data_path")
        load1 = pexpect.spawnu(config.get("GRAPHDB", "sesame_console_path"))
        load1.timeout = 300
        load1.sendline('open {0}'.format(repo_id))
        load1.expect_exact("Opened repository '{0}'".format(repo_id))
        load1.sendline('load "{0}/fhir.ttl"'.format(fhir_data_path.strip('"')))
        load1.expect("Data has been added to the repository")
        load1.sendline('load "{0}/rim.ttl"'.format(fhir_data_path.strip('"')))
        load1.expect("Data has been added to the repository")
        load1.sendline('load "{0}/w5.ttl"'.format(fhir_data_path.strip('"')))
        load1.expect("Data has been added to the repository")
        load1.sendline('close')
        load1.expect_exact("Closing repository '{0}'...".format(repo_id))
        load1.close()
        time.sleep(5)
        cnt_triples(config.get("GRAPHDB_RDFSTORE_FHIR", "get").format(repo_id=repo_id))

    if repo_id.startswith('DataCitation_CategoryLabels'):
        # create Wiki
        logging.info("Creating Wiki repository")
        create2 = pexpect.spawnu(config.get("GRAPHDB", "sesame_console_path"))
        create2.timeout = 300
        create2.sendline("create free")
        create2.sendline("{0}".format(repo_id))
        create2.sendline("Repository for Evaluation of the RDF Data Citation API")
        for k in range(18):
            create2.sendline("")
        create2.sendline("yes")
        create2.expect("Repository created")
        create2.close()

        # Load Wiki
        logging.info("Loading data into Wiki repository")
        wiki_data_path = config.get("GRAPHDB_RDFSTORE_WIKI", "data_path")
        load2 = pexpect.spawnu(config.get("GRAPHDB", "sesame_console_path"))
        load2.timeout = 300
        load2.sendline('open {0}'.format(repo_id))
        load2.expect_exact("Opened repository '{0}'".format(repo_id))
        load2.sendline('load "{0}/category_labels_wkd_uris_en.ttl"'.format(wiki_data_path.strip('"')))
        load2.expect("Data has been added to the repository")
        load2.sendline('close')
        load2.expect_exact("Closing repository '{0}'...".format(repo_id))
        load2.close()
        time.sleep(15)
        cnt_triples(config.get("GRAPHDB_RDFSTORE_WIKI", "get").format(repo_id=repo_id))


def update_triplestore(insert_statement: str, endpoint):
    sparql_post = SPARQLWrapper(endpoint)
    sparql_post.setHTTPAuth(DIGEST)
    sparql_post.setMethod(POST)
    sparql_post.setQuery(insert_statement)
    sparql_post.query()


def cnt_triples(endpoint) -> int:
    logging.info("Counting triples ...")
    logging.info(endpoint)
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


def evaluate(write_operation: str, dataset_size: str, versioning_mode: str, query_type: str,
             procedure_to_evaluate: str, output_file: str, scenario_nr: int):
    # Evaluation
    logging.info("Start evaluation with parameters: {0}, {1}, {2}, {3}, {4}".format(write_operation, dataset_size,
                                                                                    versioning_mode, query_type,
                                                                                    procedure_to_evaluate))
    # Create Repo for this scenario
    dataset_repos = {'small': 'DataCitation_FHIR', 'big': 'DataCitation_CategoryLabels'}
    repo_id = dataset_repos[dataset_size] + "_" + str(scenario_nr)
    create_repos_with_data(repo_id)

    # Init parameters for evaluation
    assert query_type in ["simple_query", "complex_query", "no_query"], "Query must be either simple, complex or none. " \
                                                                    "Latter should only be used in case of init_versioning"
    if dataset_size == "small":
        get_endpoint = config.get('GRAPHDB_RDFSTORE_FHIR', 'get').format(repo_id=repo_id)
        post_endpoint = config.get('GRAPHDB_RDFSTORE_FHIR', 'post').format(repo_id=repo_id)
        if procedure_to_evaluate != "init_versioning":
            query = open("FHIR/{0}.txt".format(query_type), "r").read()
            query_checksum = "{0}_fhir_checksum".format(query_type)
        else:
            query = "no_query"
            query_checksum = None
        init_insert_random_data = open("FHIR/insert_random_data.txt", "r").read()
    elif dataset_size == "big":
        get_endpoint = config.get('GRAPHDB_RDFSTORE_WIKI', 'get').format(repo_id=repo_id)
        post_endpoint = config.get('GRAPHDB_RDFSTORE_WIKI', 'post').format(repo_id=repo_id)
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

    # Procedures to evaluate and parameters
    procs = {'mint_query_pid': [citation.mint_query_pid, (query, metadata)],
             'init_versioning': [rdf_engine.version_all_rows, [vers_mode]],
             're-execute_query': [citation.mint_query_pid, (query, metadata)],
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

        # Remove citation from query store
        if procedure_to_evaluate == "mint_query_pid":
            query_store._remove(config.get("QUERY", query_checksum))
        elif procedure_to_evaluate == "init_versioning":
            rdf_engine.reset_all_versions()

    # Reset experiment by recreating the repositories and reloading the data
    if procedure_to_evaluate != "init_versioning":
        query_store._remove(config.get("QUERY", query_checksum))
    delete_repos(repo_id)


# Start graphdb-free
logging.info("Starting graphdb-free ...")
subprocess.Popen(['/opt/graphdb-free/graphdb-free', '-s'], shell=True, stdin=None, stdout=None,
                 stderr=None, close_fds=True)
time.sleep(15)

# Run evaluation 10 times
for i in range(10):
    logging.info("Starting run {0}".format(i))

    param_sets = set([set[:5] for set in my_index.tolist()])
    for c, param_set in enumerate(param_sets):
        logging.info("Scenario {0} starting".format(c))
        try:
            evaluate(*param_set, "evaluation_results_v20210811_{0}.csv".format(i), scenario_nr=c)
        except Exception as e:
            print(e)

# Close graphdb-free
subprocess.call(['killall', '-9', 'graphdb-free'], stdout=subprocess.PIPE, universal_newlines=True)
logging.info("Closed graph-db free")
logging.info("Evaluation finished")

