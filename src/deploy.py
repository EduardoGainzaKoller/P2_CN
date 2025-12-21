"""
Este script despliega la infraestructura necesaria en AWS usando CloudFormation.
Despliega los stacks en el orden correcto y sube el script de Glue al bucket S3.
Sin emabrgo, no inicia el crawler de Glue automáticamente. Esto se puede hacer manualmente
desde la consola de AWS o añadiendo código adicional para iniciar el crawler tras el despliegue.

"""

import boto3
import time
from loguru import logger

# CONFIGURACIÓN
REGION = 'us-east-1'
STACK_BASE = 'p2-s3-kinesis'
STACK_INGESTA = 'p2-ingestion-stack'
STACK_GLUE = 'p2-glue-stack'

cf = boto3.client('cloudformation', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

def wait_for_stack(stack_name):
    logger.info(f"Esperando a que {stack_name} se complete...")
    waiter = cf.get_waiter('stack_create_complete')
    waiter.wait(StackName=stack_name)
    logger.success(f"Stack {stack_name} desplegado con éxito.")

def deploy_stack(stack_name, template_path, parameters=[]):
    with open(template_path, 'r') as f:
        template_body = f.read()
    
    logger.info(f"Desplegando {stack_name}...")
    cf.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Parameters=parameters,
        Capabilities=['CAPABILITY_NAMED_IAM', 'CAPABILITY_AUTO_EXPAND']
    )
    wait_for_stack(stack_name)

def get_bucket_name(stack_name):
    response = cf.describe_stacks(StackName=stack_name)
    outputs = response['Stacks'][0]['Outputs']
    for output in outputs:
        if 'BucketName' in output['OutputKey']:
            return output['OutputValue']
    return None

def run():
    try:
        
        deploy_stack(STACK_BASE, 'infra\s3_kinesis.yml')
        
        bucket = get_bucket_name(STACK_BASE)
        logger.info(f"Subiendo script de Glue al bucket: {bucket}")
        s3.upload_file('etl\glue_job.py', bucket, 'config/scripts/glue_job_script.py')
        
        deploy_stack(STACK_INGESTA, 'infra/firehose.yml', [
            {'ParameterKey': 'BaseStackName', 'ParameterValue': STACK_BASE}
        ])
        
        deploy_stack(STACK_GLUE, 'infra\glue.yml', [
            {'ParameterKey': 'BaseStackName', 'ParameterValue': STACK_BASE}
        ])

        logger.success("--- TODO EL PIPELINE ESTÁ ACTIVO ---")

    except Exception as e:
        logger.error(f"Error en el despliegue: {e}")

if __name__ == "__main__":
    run()