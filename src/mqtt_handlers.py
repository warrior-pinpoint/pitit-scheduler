from .common import constants
from .rendezvous import hash as rendezvous_hash
import json


class MqttHandler:
    def __init__(self, app):
        self.app = app
        self._add_call_back()

    def _add_call_back(self):
        self.app._mqtt_client.message_callback_add(constants.CREATE_BOT_TOPIC, self.scheduler_new_bot)

    def scheduler_new_bot(self, client, message):
        payload = json.loads(message.payload)
        bot_id = payload['bot_id']
        minion_idx = rendezvous_hash(bot_id, self.app.minions.keys())
        selected_id = self.app.minions[minion_idx]['minion_id']
        data = {'minion_id': selected_id, 'action': 'assigned_minion'}
        self.app.pp_client.update_bot(bot_id, data)
        self.app.minions[minion_idx]['bots'].append(bot_id)
        print(f'Scheduled for bot {bot_id} run on minion {selected_id}')
