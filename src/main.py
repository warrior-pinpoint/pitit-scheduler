import threading
import signal
import time
import logging
from ptitnetwork import flask
from .http_client import PinpointApiClient
from ptitnetwork.mqtt.client import Mqtt as MqttClient
from . import mqtt_handlers
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Worker:

    def __init__(self, interval=1.0):
        self._interval = interval
        self._flask_client = flask.create_default_app(__name__)
        self._mqtt_client = MqttClient.create(on_connect=self._on_mqtt_connect)
        self.pp_client = PinpointApiClient()
        self._mqtt_handler = mqtt_handlers.MqttHandler(self)
        self.minions = []
        self._is_stoped = False
        self._is_active = False
        self._id = None
        self._name = None
        signal.signal(signal.SIGTERM, self.on_stop)
        signal.signal(signal.SIGINT, self.on_stop)

    def run(self):
        try:
            self.on_start_up()
            self.on_active()
            self.on_loop()
            self.on_stop(None, None)
        except Exception as e:
            logger.error("EXCEPTION: " + str(e))
            self.on_stop(None, None)

    def _setup_flask_app(self):
        pass

    def on_start_up(self):
        logger.info('On start up...')

    def on_active(self):
        logger.info('On active...')
        self._is_active = True
        self.minions = self.get_ready_minion()
        print('Minion....', self.minions)
        self._run_flask_app()
        self._mqtt_client.loop_start()

    def on_loop(self):
        logger.info('On loop...')
        while not self._is_stoped:
            time.sleep(self._interval)

    def on_stop(self, sig, frame):
        logger.info('On stop...')
        self._is_stoped = True
        self._mqtt_client.loop_stop()

    def _run_flask_app(self):
        self._setup_flask_app()
        flask_pp = threading.Thread(target=self._flask_client.run,
                                          kwargs={'debug': False, 'host': '0.0.0.0', 'port': 8083})
        flask_pp.setDaemon(True)
        flask_pp.start()

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        logger.info(f'Connected to mqtt with code {rc}')

    def get_ready_minion(self):
        print('Scheduler get ready minion...')
        minions = self.pp_client.get_ready_minion().json()
        return {idx: {'minion_id': minion['id'], 'bots': [bot['id'] for bot in minion['bots']]}
                for idx, minion in enumerate(minions)}


if __name__ == '__main__':
    worker = Worker()
    worker.run()
