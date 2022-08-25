#!/usr/bin/env python3
"""main app file"""

import aws_cdk as cdk

from aws_cdk_avro_glue.aws_cdk_avro_glue_stack import AwsCdkAvroGlueStack


app = cdk.App()
AwsCdkAvroGlueStack(app, "AwsCdkAvroGlueStack" )

app.synth()
