"""Example AWS Stack for AWS Spark Glue Job using Avro Files"""
from aws_cdk import (
    # Duration,
    Stack,
    aws_iam as iam,
    aws_glue as glue,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    CfnOutput,
    RemovalPolicy
    # aws_sqs as sqs,
)
from constructs import Construct

class AwsCdkAvroGlueStack(Stack):
    """Avro Glue Spark Job Stack, Service Role and Bucket w/ Resources and Script."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket=s3.Bucket(self
                            ,id="AVRO-GlueJob-ETL"
                            ,bucket_name='avro-awsglue-job-source'
                            ,removal_policy=RemovalPolicy.DESTROY
                            ,auto_delete_objects=True
        )

        s3deploy.BucketDeployment(self
                            ,id ="addLogicAndResources"
                            ,sources=[s3deploy.Source.asset('./gluejob_asset')]
                            ,destination_bucket=bucket
                            )

        glue_role = iam.Role(self
                            ,id='AVROGlueJobServiceRole'
                            ,role_name='AVRO-ETL-Glue-ServiceRole'
                            ,assumed_by=iam.ServicePrincipal('glue.amazonaws.com')
                            )

        bucket.grant_read_write(glue_role)

        glue_job = glue.CfnJob(self
                            ,id="AVRO-ETL"
                            ,name="AVRO-ETL-GlueJob"
                            ,command=glue.CfnJob.JobCommandProperty(
                                #For an Apache Spark ETL job, name must be glueetl
                                name="glueetl"
                            ,python_version='3'
                            ,script_location=f's3://{bucket.bucket_name}/gluejob.py'
                            )
                            ,role =glue_role.role_arn
                            ,default_arguments={'--additional-python-modules': 'fastavro==1.6.0'
                                                ,'--beg_date': 'Default'
                                                ,'--end_date': 'Default'
                                                ,'--job_name': 'AVRO-ETL-GlueJob'
                                                ,'--class': 'GlueApp'
                                                }
                            ,glue_version="3.0"
                            ,max_retries=10
                            #,max_capacity=10
                            ,timeout=10
                            ,execution_property=glue.CfnJob.ExecutionPropertyProperty(
                                max_concurrent_runs=10
                            )
                            ,worker_type='G.1X'
                            ,number_of_workers=10
                            )

        CfnOutput(self,'Avro Glue Job Details', value=f"name:{glue_job.name} created!")
