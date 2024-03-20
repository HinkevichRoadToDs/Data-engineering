import luigi
import subprocess
import os
import requests
import tarfile
import gzip
from luigi import Task, run, LocalTarget, Parameter, build
from pathlib import Path
from dotenv import load_dotenv

def initial_data():
    load_dotenv()
    source = os.getenv('source_link')
    dataset_name = os.getenv('dataset_name')
    source_link = source.format(dataset_name)
    return source_link

class GetData(Task):
    source_link = Parameter()
    def run(self):
        self.output()

    def output(self):
        response = requests.get(self.source_link)
        if response.status_code == 200:
            with open('files/archive.tar', 'wb') as file:
                file.write(response.content)
            with tarfile.open("files/archive.tar", 'r') as file:
                file.extractall("files/extracted_data")

        extracted_directory = Path('files/extracted_data')
        for file_path in Path('files/extracted_data').iterdir():

            if file_path.suffix == '.gz':
                target_file = extracted_directory / file_path.with_suffix('.txt').name
                with gzip.open(file_path, 'rt', encoding='utf-8') as f, open(target_file, 'w') as f1:
                    txt_content = f.read()
                    f1.write(txt_content)
        return luigi.LocalTarget(target_file)

class StartEngine(Task):
    def requires(self):
        return GetData()


if __name__ == '__main__':
    build([GetData(initial_data())], workers =1, local_scheduler = True)