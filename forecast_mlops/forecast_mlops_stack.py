from aws_cdk import (
    core,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_src,
    aws_lambda_python as _lambda_python,
    aws_s3 as _s3,
    aws_athena as _athena,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_ssm as ssm
)
# from aws_cdk.aws_lambda_python import PythonFunction
from iam_role import IamRole

class ForecastMlopsStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # ------ Necessary Roles ------
        roles = IamRole(
            self, 'IamRoles'
        )
        

        # ------ S3 Buckets ------
        # Create Athena bucket
        athena_bucket = _s3.Bucket(self, "AthenaBucket",
            removal_policy=core.RemovalPolicy.DESTROY
        )
        # Create Forecast bucket
        forecast_bucket = _s3.Bucket(self, "FoecastBucket",
            removal_policy=core.RemovalPolicy.DESTROY
        )


        # ------ Athena ------ 
        # Config Athena query result output location
        workgroup_prop = _athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            result_configuration=_athena.CfnWorkGroup.ResultConfigurationProperty(
                output_location="s3://"+athena_bucket.bucket_name
            )
        )
        # Create Athena workgroup
        athena_workgroup = _athena.CfnWorkGroup(
            self, 'ForecastGroup',
            name='ForecastGroup', 
            recursive_delete_option=True, 
            state='ENABLED', 
            work_group_configuration=workgroup_prop
        )
            
    
        # ------ SNS Topic ------
        topic = sns.Topic(
            self, 'NotificationTopic',
            display_name='StepsTopic'
        )
        # SNS email subscription. Get the email address from context value(cdk.json)
        topic.add_subscription(subs.EmailSubscription(self.node.try_get_context('my_email')))
         

        # ------ Layers ------
        shared_layer = _lambda.LayerVersion(
            self, 'LambdaLayer',
            layer_version_name='testfolderlayer',
            code=_lambda.AssetCode('shared/')
        )


        # ------ Lambdas for stepfuctions------
        create_dataset_lambda = _lambda.Function(
            self, 'CreateDataset',
            function_name='CreateDataset',
            code=_lambda.Code.asset('lambdas/createdataset/'),
            handler='dataset.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            timeout=core.Duration.seconds(30),
            layers=[shared_layer]
        )

        create_dataset_group_lambda = _lambda.Function(
            self, 'CreateDatasetGroup',
            function_name='CreateDatasetGroup',
            code = _lambda.Code.asset('lambdas/createdatasetgroup/'),
            handler = 'datasetgroup.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            layers=[shared_layer]
        )

        import_data_lambda = _lambda.Function(
            self, 'CreateDatasetImportJob',
            function_name='CreateDatasetImportJob',
            code = _lambda.Code.asset('lambdas/createdatasetimportjob/'),
            handler = 'datasetimport.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            environment= {
                'FORECAST_ROLE': roles.forecast_role.role_arn
            },
            layers=[shared_layer]
        )

        create_predictor_lambda = _lambda.Function(
            self, 'CreatePredictor',
            function_name='CreatePredictor',
            code = _lambda.Code.asset('lambdas/createpredictor/'),
            handler = 'predictor.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            layers=[shared_layer]
        )

        create_forecast_lambda = _lambda.Function(
            self, 'CreateForecast',
            function_name='CreateForecast',
            code = _lambda.Code.asset('lambdas/createforecast/'),
            handler = 'forecast.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            environment= {
                'EXPORT_ROLE': roles.forecast_role.role_arn
            },
            layers=[shared_layer],
            timeout=core.Duration.seconds(30)
        )

        # Deploy lambda with python dependencies from requirements.txt
        update_resources_lambda = _lambda_python.PythonFunction(
            self, 'UpdateResources',
            function_name='UpdateResources',
            entry='lambdas/updateresources/',
            index='update.py',
            handler='lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.update_role,
            environment= {
                'ATHENA_WORKGROUP': athena_workgroup.name,
                'ATHENA_BUCKET' : athena_bucket.bucket_name
            },
            layers=[shared_layer],
            timeout=core.Duration.seconds(900)
        )
        

        notify_lambda = _lambda.Function(
            self, 'NotifyTopic',
            function_name='NotifyTopic',
            code = _lambda.Code.asset('lambdas/notify/'),
            handler = 'notify.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            environment= {
                'SNS_TOPIC_ARN': topic.topic_arn
            },
            layers=[shared_layer]
        )

        delete_forecast_lambda = _lambda.Function(
            self, 'DeleteForecast',
            function_name='DeleteForecast',
            code = _lambda.Code.asset('lambdas/deleteforecast/'),
            handler = 'deleteforecast.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            layers=[shared_layer]
        )

        delete_predctor_lambda = _lambda.Function(
            self, 'DeletePredictor',
            function_name='DeletePredictor',
            code = _lambda.Code.asset('lambdas/deletepredictor/'),
            handler = 'deletepredictor.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            layers=[shared_layer]
        )

        delete_importjob_lambda = _lambda.Function(
            self, 'DeleteImportJob',
            function_name='DeleteImportJob',
            code = _lambda.Code.asset('lambdas/deletedatasetimport/'),
            handler = 'deletedataset.lambda_handler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            role=roles.lambda_role,
            layers=[shared_layer]
        )


        # ------ StepFunctions ------
        strategy_choice = sfn.Choice(
            self, 'Strategy-Choice'
        )

        success_state = sfn.Succeed(
            self, 'SuccessState'
        )

        failed = sfn_tasks.LambdaInvoke(
            self, 'Failed',
            lambda_function = notify_lambda,
            result_path=None
        ).next(strategy_choice)

        create_dataset_job = sfn_tasks.LambdaInvoke(
            self, 'Create-Dataset', 
            lambda_function = create_dataset_lambda,
            retry_on_service_exceptions=True,
            payload_response_only=True
        )

        self.add_retry_n_catch(create_dataset_job, failed)

        create_dataset_group_job = sfn_tasks.LambdaInvoke(
            self, 'Create-DatasetGroup', 
            lambda_function = create_dataset_group_lambda,
            payload_response_only=True
        )
        self.add_retry_n_catch(create_dataset_group_job, failed)


        import_data_job = sfn_tasks.LambdaInvoke(
            self, 'Import-Data',
            lambda_function = import_data_lambda,
            payload_response_only=True
        )
        self.add_retry_n_catch(import_data_job, failed)

        create_predictor_job = sfn_tasks.LambdaInvoke(
            self, 'Create-Predictor',
            lambda_function = create_predictor_lambda,
            payload_response_only=True
        )
        self.add_retry_n_catch(create_predictor_job, failed)

        create_forecast_job = sfn_tasks.LambdaInvoke(
            self, 'Create-Forecast',
            lambda_function = create_forecast_lambda,
            payload_response_only=True
        )
        self.add_retry_n_catch(create_forecast_job, failed)

        update_resources_job = sfn_tasks.LambdaInvoke(
            self, 'Update-Resources',
            lambda_function = update_resources_lambda,
            payload_response_only=True
        )
        self.add_retry_n_catch(update_resources_job, failed)

        notify_success = sfn_tasks.LambdaInvoke(
            self, 'Notify-Success',
            lambda_function = notify_lambda,
            payload_response_only=True
        )

        delete_forecast_job = sfn_tasks.LambdaInvoke(
            self, 'Delete-Forecast',
            lambda_function = delete_forecast_lambda,
            payload_response_only=True
        )
        self.delete_retry(delete_forecast_job)

        delete_predictor_job = sfn_tasks.LambdaInvoke(
            self, 'Delete-Predictor',
            lambda_function = delete_predctor_lambda,
            payload_response_only=True
        )
        self.delete_retry(delete_predictor_job)

        delete_import_job = sfn_tasks.LambdaInvoke(
            self, 'Delete-ImportJob',
            lambda_function = delete_importjob_lambda,
            payload_response_only=True
        )
        self.delete_retry(delete_import_job)
        
        
        definition = create_dataset_job\
            .next(create_dataset_group_job)\
            .next(import_data_job)\
            .next(create_predictor_job)\
            .next(create_forecast_job)\
            .next(update_resources_job)\
            .next(notify_success)\
            .next(strategy_choice.when(sfn.Condition.boolean_equals('$.params.PerformDelete', False), success_state)\
                                .otherwise(delete_forecast_job).afterwards())\
            .next(delete_predictor_job)\
            .next(delete_import_job)
                    
            
        deployt_state_machine = sfn.StateMachine(
            self, 'StateMachine',
            definition = definition
            # role=roles.states_execution_role
        )

        # S3 event trigger lambda
        s3_lambda = _lambda.Function(
            self, 'S3Lambda',
            function_name='S3Lambda',
            code=_lambda.Code.asset('lambdas/s3lambda/'),
            handler='parse.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_7,
            role=roles.trigger_role,
            environment= {
                'STEP_FUNCTIONS_ARN': deployt_state_machine.state_machine_arn,
                'PARAMS_FILE': self.node.try_get_context('parameter_file')
            }
        )
        s3_lambda.add_event_source(
            event_src.S3EventSource(
                bucket=forecast_bucket,
                events=[_s3.EventType.OBJECT_CREATED],
                filters=[_s3.NotificationKeyFilter(
                    prefix='train/',
                    suffix='.csv'
                )]
            )
        )

        # CloudFormation output
        core.CfnOutput(
            self, 'StepFunctionsName',
            description='Step Functions Name',
            value=deployt_state_machine.state_machine_name
        )

        core.CfnOutput(
            self, 'ForecastBucketName',
            description='Forecast bucket name to drop you files',
            value=forecast_bucket.bucket_name
        )

        core.CfnOutput(
            self, 'AthenaBucketName',
            description='Athena bucket name to drop your files',
            value=athena_bucket.bucket_name
        )
    
    def add_retry_n_catch(self, task:sfn_tasks.LambdaInvoke, catch_handler:sfn.IChainable):
        task.add_retry(errors=['ResourcePending'], 
                interval=core.Duration.seconds(1),
                backoff_rate=1.5,
                max_attempts=100
        ).add_catch(handler=catch_handler,
                errors=['ResourceFailed'],
                result_path='$.statesError'
        ).add_catch(handler=catch_handler,
                errors=['States.ALL'],
                result_path='$.statesError'
        )
        
    def delete_retry(self, task:sfn_tasks.LambdaInvoke):
        task.add_retry(errors=['ResourcePending'], 
                interval=core.Duration.seconds(2),
                backoff_rate=2.0,
                max_attempts=100
        )