from wikiCat.processors.processor import Processor
from dateutil import parser
import findspark
findspark.init()
from pyspark.sql import Row
import shutil
import os

class SparkProcessorParsed(Processor):
    def __init__(self, project):
        Processor.__init__(self, project, 'parsed')

    # Function fo parse node information into a DataFrame
    # Returns DataFrame with Columns: id, title, ns
    def mapper_page_info(self, line):
        fields = line.split('\t')
        page_id = int(fields[0])
        page_title = str(fields[1])
        page_ns = int(fields[2])
        if page_ns == 14:
            page_title = page_title[9:]
        return Row(page_id=page_id, page_title=page_title, page_ns=page_ns)

    # Function fo parse edge information into a DataFrame
    # Returns DataFrame with Columns: source_id, target_title, rev_id
    def mapper_page_data(self, line):
        fields = line.split('\t')
        source_id = int(fields[0])
        rev_id = int(fields[1])
        target_title = str(fields[2])
        return Row(source_id=source_id, target_title=target_title, rev_id=rev_id)

    # Function fo parse revision information into a DataFrame
    # Returns Key-Value-Pair with the revision ID as key and revision TIME as value
    def mapper_revisions(self, line):
        fields = line.split('\t')
        rev_id = int(fields[1])
        rev_date = parser.parse(fields[2])
        return Row(rev_id=rev_id, rev_date=rev_date)

    def handle_spark_results(self, path, file):
        spark_path = os.path.join(path, file)
        for filename in os.listdir(spark_path):
            if filename.endswith('.csv'):
                shutil.copyfile(os.path.join(spark_path, filename), os.path.join(path, filename))
                shutil.rmtree(spark_path)
                os.rename(os.path.join(path, filename), os.path.join(path, file))

    def assemble_spark_results(self, path, results_file):
        for file in next(os.walk(os.getcwd()+path))[2]:
            if file[0] != '.':
                with open(os.getcwd()+results_file, 'a') as out:
                    with open(os.getcwd()+path + file, 'r') as infile:
                        try:
                            for line in infile.readlines():
                                fields = line.split('\t')
                                for i in range(len(fields)):
                                    if fields[i] == '':
                                        fields[i] = 'NaN'
                                new_line = '\t'.join(map(str, fields))
                                out.write(new_line)  # +'\n')
                        except:
                            pass
        shutil.rmtree(os.getcwd()+path)
