import os
import pandas as pd


def preprocess(dir: str):
    """
    # Convert statements to n3() format and write to datasets.
    # E.g. http://data.ontotext.com/publishing#Mention-dbaa4de4563be5f6b927c87e09f90461c09451296f4b52b1f80dcb6e941a5acd>
    # -->
    # <http://data.ontotext.com/publishing#Mention-dbaa4de4563be5f6b927c87e09f90461c09451296f4b52b1f80dcb6e941a5acd>"

    :param dir: Diretory where the datasets are
    :return:
    """
    for filename in os.listdir(dir):
        if filename.startswith("dataset") and filename.endswith(".csv") and not filename.endswith('_preprocessed.csv'):
            df = pd.read_csv(dir + filename, dtype={'s': str, 'p': str, 'new_label': str})
            df.s = "<" + df.s + ">"
            df.p = "<" + df.p + ">"
            df.new_label = "'" + df.new_label + "'"
            df.to_csv(dir + filename + "_preprocessed.csv", index=False)
        if filename == 'init_version_dataset.csv' and not filename.endswith('_preprocessed.csv'):
            df = pd.read_csv(dir + filename, dtype={'s': str, 'p': str, 'o': str})
            df.s = "<" + df.s + ">"
            df.p = "<" + df.p + ">"
            df.o = "'" + df.o + "'"
            df.to_csv(dir + filename + "_preprocessed.csv", index=False)


dir_fhir = "FHIR/sample sets/"
dir_wiki = "Wikipedia/sample sets/"
preprocess(dir_fhir)
preprocess(dir_wiki)
