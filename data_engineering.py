import luigi
import subprocess
import os
import requests
import tarfile
import gzip
import glob
import io
import pandas as pd
from luigi import Task, run, LocalTarget, Parameter, build
from pathlib import Path
from dotenv import load_dotenv


class GetData(Task):
    source_link = Parameter()
    def run(self):
        response = requests.get(self.source_link)
        if response.status_code == 200:
            with open('files/archive.tar', 'wb') as file:
                file.write(response.content)
            with tarfile.open("files/archive.tar", 'r') as file:
                file.extractall("files/extracted_data")

        extracted_directory = Path('files/extracted_data')
        count = 0
        for file_path in Path('files/extracted_data').iterdir():

            if file_path.suffix == '.gz':
                target_file = extracted_directory / Path(f'txt_data_{count}.txt')
                count += 1
                with gzip.open(file_path, 'rt', encoding='utf-8') as f, open(target_file, 'w') as f1:
                    txt_content = f.read()

                    f1.write(txt_content)
        files_to_delete = glob.glob(os.path.join('files/extracted_data', '*.txt.gz'))
        for file in files_to_delete:
            os.remove(file)

        with self.output().open('w') as f:
            f.write("GetData completed")

    def output(self):
        return luigi.LocalTarget('files/extracted_data/GetData completed.txt')

class ExtractDataframes(Task):
    source_link = Parameter()
    def requires(self):
        return GetData(self.source_link)
    def output(self):
        # with open('files/extracted_data/Probes_data.tsv', 'w') as f:
        #     pass
        return LocalTarget('files/extracted_data/flow completed.txt')
    def run(self):
        for file_path in Path('files/extracted_data').iterdir():
            if file_path.suffix == '.txt':
                dfs = {}
                with open(file_path) as f:
                    write_key = None
                    fio = io.StringIO()
                    for l in f.readlines():
                        if l.startswith('['):
                            if write_key:
                                fio.seek(0)
                                header = None if write_key == 'Heading' else 'infer'
                                dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                            fio = io.StringIO()
                            write_key = l.strip('[]\n')
                            continue
                        if write_key:
                            fio.write(l)
                    fio.seek(0)
                    dfs[write_key] = pd.read_csv(fio, sep='\t')
                    df_probes = dfs['Probes'].drop(
                        ['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms',
                         'Obsolete_Probe_Id', 'Probe_Sequence'], axis=1)
                    df_probes.to_csv("files/extracted_data/Probes_data.tsv", sep=" ")
                    with self.output().open('w') as f:
                        f.write("flow completed")


class StartEngine(Task):

    def requires(self):
        def initial_data():
            load_dotenv()
            source = os.getenv('source_link')
            dataset_name = os.getenv('dataset_name')
            source_link = source.format(dataset_name)
            return source_link
        return [ExtractDataframes(initial_data())]


if __name__ == '__main__':
    build([StartEngine()], workers =1, local_scheduler = True)
