import logging

from faust import App

app = App('kafka_shipper', broker='kafka', value_serializer='raw')
logger = logging.getLogger('kafka_shipper')

pipe_in = app.topic('kafka_shipper_in')

app.main()
