"""example aws glue etl spark job w/ avro files"""
import sys
from datetime import datetime,timedelta
from io import BytesIO, StringIO
from typing import List, Optional
import boto3
from fastavro import reader
from pyspark.sql import SparkSession, DataFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

MY_SOURCE_BUCKET='avro-awsglue-job-source'

class AvroGlueJob:
    '''Reads information from avro files and returns an ETL csv'''
    def __init__(self,args: List[str]):
        '''get the parameters values passed to job and initilaze GlueContext'''
        self.args = getResolvedOptions(sys.argv,args)
        self.glue_context = GlueContext(SparkContext())


    def datetime_format(self, date_time: datetime, day_offset:int = 0, mod:bool =True)->str:
        '''datetime_format: Returns a formatted date string object as YYYY-MM-DD HH:MM:SS
        :params:
            date_time  (datetime) - datetime object to be formatted
            day_offset (int)      - offset numeric value to be added to date_time
            mod        (bool)     - Boolean value that dictates if H,M,S will be set to 0

        :return:
            strftime   (str)      - formatted date string object
        '''
        if mod:
            date_mod= date_time.replace(hour=0,minute=0,second=0, \
                microsecond=0,tzinfo=None) \
                +timedelta(days=day_offset)
            return date_mod.strftime('%Y-%m-%d %H:%M:%S')
        return date_time.strftime('%Y-%m-%d %H:%M:%S')


    def avro_reader(self, obj) ->List[dict]:
        '''avro_reader: Decodes the s3 ObjectSummary Dict
        :params:
            obj (boto3.) - Object that contains the avro Bytes data

        :returns:
            records (List[dict]) - Returns a list of decoded avro files where each record is
            a dict object.
        '''
        file = obj.get()['Body'].read()
        rb = BytesIO(file)
        rb.seek(0)
        avro_file = reader(rb)
        records=[]
        for record in avro_file:
            records.append(record)
        return records



    def avro_sources(self,beg_date: str =None, end_date: str= None):
        '''avro_sources: selects avro files by last modified date to be used in ETL.
        Only avro files last modified between Begin Date(inclusvie) and End Date (exclusive)
        are selected.

        :params:
            beg_date (string) - Date string object formatted as YYYY-MM-DD HH:MM:SS.
            Defaults to Today if No value is specified.

            end_date (string) - Date string object formatted as YYYY-MM-DD HH:MM:SS.
            Defaults to Tomorrow if No value is specified.

        :returns:
            records (List[dict]) - Returns a list of decoded avro files where each record is
            a dict object.
        '''
        if beg_date == 'Default':
            beg_date = self.datetime_format(datetime.today())

        if end_date == 'Default':
            end_date= self.datetime_format(datetime.today(),day_offset=1)

        print(f"avro_sources: beg_date:{beg_date} ({len(end_date)})")
        print(f"avro_sources: end_date:{end_date} ({len(end_date)})")
        s3= boto3.resource('s3')
        avro_bucket= s3.Bucket(MY_SOURCE_BUCKET)
        records=[]
        for obj in avro_bucket.objects.filter(Prefix='avro'):
            obj_modtime=self.datetime_format(obj.last_modified,mod=False)
            #get the .avro files between dates..
            if '.avro' in obj.key and (obj_modtime>=beg_date and obj_modtime<end_date):
                #decode bytes...
                file_records = self.avro_reader(obj)
                records.extend(file_records)

        return records

    def extract_fields(self, records: List[dict] ,*args: Optional[str]) ->List[dict]:
        '''extract_fields: extracts data from nested fields in avro file and merges into
        a single dict per record.

        :params:
            records (List[dict]) - recieves output from self.avro_sources.

            args    (Optional[str]) - nested fields that are to be merged.

        :returns:
            format_records (List[dict]) - Returns a list of dicts where each record is
            an avro file record.

        '''
        format_records=[]
        for row in records:
            res = row.copy()
            extract={}
            for x in args:
                for key,value in res[x].items():
                    new_key=x+'_'+key
                    extract[new_key] = value
            #extract[x]=res[x]
            for x in args:
                del res[x]
            format_records.append({**res, **extract})
        return format_records

    def create_df(self, data: List[dict] , *args: Optional[str]) -> DataFrame:
        '''create_df: reads the decoded avro file data and creates spark df

        :params:
            data (List[dict]) - accepts output from self.extract_fields()

            args (Optional[str]) - nested fields that are to be merged.
            Args is passed into self.extract_fields().

        :returns:
            df1 (DataFrame) - returns Spark DataFrame Object.
        '''
        spark = SparkSession.builder.appName('avro.df').getOrCreate()
        etl=self.extract_fields(data,*args)
        df1 = spark.createDataFrame(etl)
        return df1

    def generate_key(self, name:str )->str:
        '''generate_key: creates name and timestamp csv file name

        :params:
            name - prefix to timestamp name.

        :returns:
            key - formatted key name.
        '''

        time = datetime.now().strftime('%Y%m%d_%H%M%S')
        key= name+'_'+time+'.csv'
        return key

    def output_csv(self, df: DataFrame, name: str) -> bool:
        '''output_csv: save the transformed df into csv in target bucket

        :params:
            df (DataFrame) - dataframe

        :returns:
            True (Bool) '''
        s3= boto3.resource('s3')
        target = s3.Bucket(MY_SOURCE_BUCKET)
        buffer = StringIO()
        df.toPandas().to_csv(buffer, index=False)
        key_name=self.generate_key(name)
        target.put_object(Body=buffer.getvalue(), Key=key_name)
        return True

    def run_job(self)->None:
        '''run_job: Runs Glue ETL avro job

        :params:
            None

        :returns:
            None
        '''
        job = Job(self.glue_context)
        job.init(self.args['job_name'],self.args)
        begin_date = self.args['beg_date']
        ending_date =self.args['end_date']
        print(f'begin_date:{begin_date} ending_date:{ending_date}')
        data = self.avro_sources(beg_date=begin_date, end_date=ending_date)
        df = self.create_df(data,'decoded_rate_token')
        self.output_csv(df,'test-avro')
        job.commit()


if __name__ == '__main__':
    #these are the arguments I want to use in my glue job
    my_args =['beg_date','end_date','job_name']
    my_job = AvroGlueJob(my_args)
    my_job.run_job()
