from SPARQLWrapper import SPARQLWrapper, DIGEST, POST
from rdf_data_citation import citation as ct
from rdf_data_citation import citation_utils as ct_ut
from rdf_data_citation import rdf_star as rdf
from rdf_data_citation.rdf_star import VersioningMode
from rdf_data_citation import query_store as qs
import configparser
import logging
import timeit
import resource
import time
from memory_profiler import profile
from memory_profiler import memory_usage
import csv
import os
import pandas as pd
from rdflib import Literal, URIRef


config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))

# Setup
write_operations = ['insert', 'update']
dataset_types = ['small', 'big']
versioning_modes = ['q_perf', 'mem_sav']
query_types = ['simple_query', 'complex_query']
evaluation_steps = ['init_versioning', 'cite_query', 're-cite_query', 'retrieve_live_data', 'retrieve_history_data']
rounds = list(range(1, 11))
my_index = pd.MultiIndex.from_product(iterables=[rounds, write_operations, dataset_types, versioning_modes,
                                                 query_types, evaluation_steps],
                                      names=['round', 'write_operation', 'dataset', 'versioning_mode', 'query_type',
                                             'evaluation_step'])
eval_results = pd.DataFrame(columns=['memory', 'time'],
                            index=my_index)


# Init query store and RDF store
query_store = qs.QueryStore()
rdf_engine = rdf.TripleStoreEngine(config.get('GRAPHDB_RDFSTORE_FHIR', 'get'),
                                   config.get('GRAPHDB_RDFSTORE_FHIR', 'post'))


# FHIR
simple_query_fhir = open("FHIR/simple_query.txt", "r").read()
complex_query_fhir = open("FHIR/complex_query.txt", "r").read()

citation = ct.Citation(config.get('GRAPHDB_RDFSTORE_FHIR', 'get'), config.get('GRAPHDB_RDFSTORE_FHIR', 'post'))
metadata = ct_ut.MetaData(identifier="simple_query_fhir_eval_20210602143900",
                          publisher="Filip Kovacevic",
                          resource_type="RDF",
                          publication_year="2021",
                          creator="Filip Kovacevic",
                          title="Simple FHIR query")


def update_triplestore(insert_statement: str):
    sparql_post = SPARQLWrapper(config.get('GRAPHDB_RDFSTORE_FHIR', 'post'))
    sparql_post.setHTTPAuth(DIGEST)
    sparql_post.setMethod(POST)
    sparql_post.setQuery(insert_statement)
    sparql_post.query()


# Evaluation
print("Start evaluation with parameters: insert, small, mem_sav, simple query, cite_query")

rdf_engine.version_all_rows(versioning_mode=VersioningMode.SAVE_MEM)
for i in range(1, 11):
    # Perform action and measure time and memory
    time_start = time.perf_counter()
    citation.cite(simple_query_fhir, metadata)
    time_elapsed = (time.perf_counter() - time_start)
    memMb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0 / 1024.0

    # Insert new random triples with fixed dataset size (10% of initial superset)
    insert_random_data = open("FHIR/insert_random_data.txt", "r").read()
    update_triplestore(insert_random_data)

    # Save evaluation results
    eval_results.loc[(i, 'insert', 'small', 'mem_sav', 'simple_query', 'cite_query')] = [time_elapsed, memMb]

    # Remove citation from query store
    query_store._remove(config.get("QUERY", 'simple_query_fhir_checksum'))

# Save evaluation results to csv
eval_results.to_csv()

# Reset experiment environment and settings
delete_random_data = open("FHIR/delete_random_data.txt", "r").read()
update_triplestore(delete_random_data)
rdf_engine.reset_all_versions()

