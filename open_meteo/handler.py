"""AWS Lambda entry point. Handler path: open_meteo.handler.handler"""

from .extract import run


def handler(event, context):
    return run(event or {})
