from wikiCat.processor.spark_processor import SparkProcessor
from dateutil import parser
import shutil
import os
import findspark
findspark.init()
from pyspark.sql import Row


class SparkProcessorParsed(SparkProcessor):
    def __init__(self, project):
        SparkProcessor.__init__(self, project, 'parsed')

    # Function to parse node information into a DataFrame
    # Returns DataFrame with Columns: id, title, ns
    @staticmethod
    def mapper_page_info(line):
        fields = line.split('\t')
        page_id = int(fields[0])
        page_title = str(fields[1])
        page_ns = int(fields[2])
        return Row(page_id=page_id, page_title=page_title, page_ns=page_ns)

    # Function to parse edge information into a DataFrame
    # Returns DataFrame with Columns: source_id, target_title, rev_id
    @staticmethod
    def mapper_page_data(line):
        fields = line.split('\t')
        source_id = int(fields[0]) #todo DEBUGGING NODE CREATION ERROR: Potentially was str
        rev_id = int(fields[1])
        target_title = str(fields[2])
        return Row(source_id=source_id, target_title=target_title, rev_id=rev_id)

    # Function to parse revision information into a DataFrame
    # Returns Key-Value-Pair with the revision ID as key and revision TIME as value
    @staticmethod
    def mapper_revisions(line):
        fields = line.split('\t')
        rev_id = int(fields[1])
        rev_date = parser.parse(fields[2])
        rev_date = rev_date.timestamp()
        rev_author = int(float(str(fields[3])))
        return Row(rev_id=rev_id, rev_date=rev_date, rev_author=rev_author)

    # Function fo parse author information into a DataFrame
    # Returns DataFrame with the Columns: author ID, author NAME
    @staticmethod
    def mapper_author_info(line):
        fields = line.split('\t')
        author_id = int(float(str(fields[0])))
        author_name = str(fields[1])
        return Row(author_id=author_id, author_name=author_name)

    @staticmethod
    def handle_spark_results(path, file):
        spark_path = os.path.join(path, file)
        for filename in os.listdir(spark_path):
            if filename.endswith('.csv'):
                shutil.copyfile(os.path.join(spark_path, filename), os.path.join(path, filename))
                shutil.rmtree(spark_path)
                os.rename(os.path.join(path, filename), os.path.join(path, file))

    @staticmethod
    def assemble_spark_results(path, results_file):
        #print(os.path.join(os.getcwd(), path))
        #print(os.path.isdir(os.path.join(os.getcwd(), path)))
        for file in next(os.walk(os.path.join(os.getcwd(), path)))[2]:
            if file[0] != '.':
                with open(os.path.join(os.getcwd(), results_file), 'a') as out:
                    with open(os.path.join(os.getcwd(), path) + file, 'r') as infile:
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
        shutil.rmtree(os.path.join(os.getcwd(), path))
