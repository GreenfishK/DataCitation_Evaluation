from rdf_data_citation import citation as ct
from rdf_data_citation import citation_utils as ct_ut
from rdf_data_citation import rdf_star as rdf
from rdf_data_citation.rdf_star import VersioningMode
from rdf_data_citation import query_store as qs
import configparser
import logging
import timeit
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


# Init versioning
rdf_engine = rdf.TripleStoreEngine(config.get('GRAPHDB_RDFSTORE_FHIR', 'get'),
                                   config.get('GRAPHDB_RDFSTORE_FHIR', 'post'))
rdf_engine.version_all_rows(versioning_mode=VersioningMode.SAVE_MEM)

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
dir = "FHIR/sample sets/"
all_new_triples = []
for filename in os.listdir(dir):
    if filename.startswith("dataset") and filename.endswith("_preprocessed.csv"):
        df = pd.read_csv(dir + filename)
        triples = df.values.tolist()
        all_new_triples.append(triples)
        time = timeit.timeit(lambda: citation.cite(simple_query_fhir, metadata), number=1)
        print("{0} loops, best of {1}: {2} sec per loop".format(1, 1, time))
        rdf_engine.insert_triples(triples)
rdf_engine._delete_triples(all_new_triples)

# Reset experiment environment and settings
query_store = qs.QueryStore()
query_store._remove(config.get("QUERY", 'simple_query_fhir_checksum'))
rdf_engine.reset_all_versions()

