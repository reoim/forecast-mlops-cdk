#!/usr/bin/env python3

from aws_cdk import core

from forecast_mlops.forecast_mlops_stack import ForecastMlopsStack


app = core.App()
ForecastMlopsStack(app, "forecast-mlops", env={'region': 'us-west-2'})

app.synth()
