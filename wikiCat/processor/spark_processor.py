from wikiCat.processor.processor import Processor
from dateutil import parser
import findspark
findspark.init()
from pyspark.sql import Row
import shutil
import os


class SparkProcessor(Processor):
    def __init__(self, project, dtype):
        Processor.__init__(self, project, dtype)
        self.project = project

    # Function fo parse node information into a DataFrame
    # Returns DataFrame with Columns: id, title, ns
    @staticmethod
    def mapper_page_info(line):
        fields = line.split('\t')
        page_id = int(fields[0])
        page_title = str(fields[1])
        page_ns = int(fields[2])
        if page_ns == 14:
            page_title = page_title[9:]
        return Row(page_id=page_id, page_title=page_title, page_ns=page_ns)

    # Function fo parse edge information into a DataFrame
    # Returns DataFrame with Columns: source_id, target_title, rev_id
    @staticmethod
    def mapper_page_data(line):
        fields = line.split('\t')
        source_id = int(fields[0])
        rev_id = int(fields[1])
        target_title = str(fields[2])
        return Row(source_id=source_id, target_title=target_title, rev_id=rev_id)

    # Function fo parse revision information into a DataFrame
    # Returns Key-Value-Pair with the revision ID as key and revision TIME as value
    @staticmethod
    def mapper_revisions(line):
        fields = line.split('\t')
        rev_id = int(fields[1])
        rev_date = parser.parse(fields[2])
        rev_date = rev_date.timestamp()
        return Row(rev_id=rev_id, rev_date=rev_date)

    # Function fo parse revision information into a DataFrame
    # Returns Key-Value-Pair with the revision ID as key and revision TIME as value
    @staticmethod
    def mapper_nodes(line):
        fields = line.split('\t')
        print(fields)
        if len(fields) == 3:
            id = fields[0]
            title = fields[1]
            ns = fields[2]
            return Row(id=id, title=title, ns=ns)
        elif len(fields) == 4:
            id = fields[0]
            title = fields[1]
            ns = fields[2]
            cscore = fields[3]
            return Row(id=id, title=title, ns=ns, cscore=cscore)

    @staticmethod
    def mapper_edges(line):
        fields = line.split('\t')
        if len(fields) == 3:
            source = fields[0]
            target = fields[1]
            etype = fields[2]
            return Row(source=source, target=target, etype=etype)
        elif len(fields) == 4:
            source = fields[0]
            target = fields[1]
            etype = fields[2]
            cscore = fields[3]
            return Row(source=source, target=target, etype=etype, cscore=cscore)

    @staticmethod
    def mapper_tmp_cscore_events(line):
        fields = line.split('\t')
        revision = fields[0]
        source = fields[1]
        target = fields[2]
        cscore = fields[3]
        return Row(revision=revision, source=source, target=target, cscore=cscore)

    @staticmethod
    def mapper_events(line):
        fields = line.split('\t')
        if len(fields) == 4:
            revision = float(fields[0])
            source = fields[1]
            target = fields[2]
            event = fields[3]
            return Row(revision=revision, source=source, target=target, event=event)
        elif len(fields) == 5:
            revision = float(fields[0])
            source = fields[1]
            target = fields[2]
            event = fields[3]
            cscore = fields[4]
            return Row(revision=revision, source=source, target=target, event=event, cscore=cscore)
        else:
            print('Error while mapping events')
            return

    @staticmethod
    def mapper_ids(line):
        fields = line.split('\t')
        wiki_id = fields[0]
        gt_id = fields[1]
        return Row(wiki_id=wiki_id, gt_id=gt_id)

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
        for file in next(os.walk(os.path.join(os.getcwd(), path)))[2]:
            if file[0] != '.':
                with open(os.path.join(os.getcwd(), results_file), 'a') as out:
                    with open(os.path.join(os.getcwd(), path, file), 'r') as infile:
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