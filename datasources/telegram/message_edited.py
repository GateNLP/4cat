from telethon import types
from telethon.events.common import EventBuilder, EventCommon


class ForwardedMessage(EventBuilder):
	@classmethod
	def build(cls, update, others=None, self_id=None):
		if isinstance(update, types.UpdateChannelMessageForwards):
			peer = types.PeerChannel = update.channel_id
			return cls.Event(update, peer)

	class Event(EventCommon):
		def __init__(self, update, peer ):
			super().__init__(chat_peer=peer)
			self.original_update = update
