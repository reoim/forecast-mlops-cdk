from aws_cdk import (
    aws_iam as _iam,
    core,
)


class IamRole(core.Construct):
    
    @property
    def trigger_role(self):
        return self._trigger_role

    @property
    def forecast_role(self):
        return self._forecast_role

    @property
    def lambda_role(self):
        return self._lambda_role

    @property
    def update_role(self):
        return self._update_role

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        # --- Define iam roles and policies here ---

        # --- trigger role ---
        # Create trigger role        
        self._trigger_role = _iam.Role(
            self, 'TriggerRole',
            role_name='TriggerRole',
            assumed_by=_iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[_iam.ManagedPolicy.from_aws_managed_policy_name('AWSStepFunctionsFullAccess'),
                                _iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchFullAccess'),
                                _iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess')]
        )

        # Create trigger execution policy
        trigger_execution_policy = _iam.Policy(
            self, 'TriggerExecutionPolicy',
            policy_name='TriggerExecutionPolicy',
            statements=[_iam.PolicyStatement(
                                    resources=['*'],
                                    actions=[   'lambda:InvokeFunction', 
                                                'states:*',
                                                's3:*',
                                                ]),
                        _iam.PolicyStatement(
                                    resources=['arn:aws:logs:*:*:*'],
                                    actions=['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'])]
        )

        # Attach the trigger execution policy to the trigger role
        self._trigger_role.attach_inline_policy(trigger_execution_policy)
        

        # --- forecast role ---
        # Create forecast role
        self._forecast_role = _iam.Role(
            self, 'ForecastRole',
            role_name='ForecastRole',
            assumed_by=_iam.ServicePrincipal('forecast.amazonaws.com'),
            managed_policies=[_iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchFullAccess'),
                                _iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess')]
        )


        # --- lambda role ---
        # Create lambda role
        self._lambda_role = _iam.Role(
            self, 'LambdaRole',
            role_name='LambdaRole',
            assumed_by=_iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[  _iam.ManagedPolicy.from_aws_managed_policy_name('AmazonForecastFullAccess'),
                                _iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchFullAccess')]
            )

        # create lambda execution policy
        lambda_execution_policy = _iam.Policy(
            self, 'LambdaExecutionPolicy',
            policy_name='LambdaExecutionPolicy',
            statements=[_iam.PolicyStatement(
                                    resources=['*'],
                                    actions=['lambda:InvokeFunction', 'forecast:*']),
                        _iam.PolicyStatement(
                                    resources=[self._forecast_role.role_arn],
                                    actions=['iam:PassRole'])]
        )

        # Attach the lambda execution policy to the lambda role
        self._lambda_role.attach_inline_policy(lambda_execution_policy)


        # --- Update role ---
        # Create update role
        self._update_role = _iam.Role(
            self, 'UpdateRole',
            role_name='UpdateRole',
            assumed_by=_iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[  _iam.ManagedPolicy.from_aws_managed_policy_name('AmazonAthenaFullAccess'),
                                _iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                                _iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchFullAccess')]
            )


