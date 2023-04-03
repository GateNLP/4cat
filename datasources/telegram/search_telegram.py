"""
Search Telegram via API
"""
import functools
import traceback
import datetime
import hashlib
import asyncio
import json
import time
import re
import uuid

from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from zipfile import ZipFile

from backend.abstract.search import Search
from common.lib.exceptions import QueryParametersException, ProcessorInterruptedException, ProcessorException, \
    QueryNeedsFurtherInputException
from common.lib.helpers import convert_to_int, UserInput
from datasources.telegram.message_edited import ForwardedMessage

from datetime import datetime
from telethon import TelegramClient, events, utils, types
from telethon.errors.rpcerrorlist import UsernameInvalidError, TimeoutError, ChannelPrivateError, BadRequestError, \
    FloodWaitError, ApiIdInvalidError, PhoneNumberInvalidError, UsernameNotOccupiedError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import User

import common.config_manager as config


class SearchTelegram(Search):
    """
    Search Telegram via API
    """
    type = "telegram-search"  # job ID
    category = "Search"  # category
    title = "Telegram API search"  # title displayed in UI
    description = "Scrapes messages from open Telegram groups via its API."  # description displayed in UI
    extension = "ndjson"  # extension of result file, used internally and in UI
    is_local = False  # Whether this datasource is locally scraped
    is_static = False  # Whether this datasource is still updated

    # cache
    details_cache = None
    failures_cache = None
    eventloop = None
    flawless = True
    end_if_rate_limited = 600  # break if Telegram requires wait time above number of seconds

    max_workers = 10
    max_retries = 3

    options = {
        "intro": {
            "type": UserInput.OPTION_INFO,
            "help": "Messages are scraped in reverse chronological order: the most recent message for a given entity "
                    "(e.g. a group) will be scraped first.\n\nTo query the Telegram API, you need to supply your [API "
                    "credentials](https://my.telegram.org/apps). 4CAT at this time does not support two-factor "
                    "authentication for Telegram."
        },
        "api_id": {
            "type": UserInput.OPTION_TEXT,
            "help": "API ID",
            "cache": True,
        },
        "api_hash": {
            "type": UserInput.OPTION_TEXT,
            "help": "API Hash",
            "cache": True,
        },
        "api_phone": {
            "type": UserInput.OPTION_TEXT,
            "help": "Phone number",
            "cache": True,
            "default": "+xxxxxxxxxx"
        },
        "divider": {
            "type": UserInput.OPTION_DIVIDER
        },
        "query-intro": {
            "type": UserInput.OPTION_INFO,
            "help": "You can collect messages from up to **25** entities (channels or groups) at a time. Separate with "
                    "commas or line breaks."
        },
        "query": {
            "type": UserInput.OPTION_TEXT_LARGE,
            "help": "Entities to scrape",
            "tooltip": "Separate with commas or line breaks."
        },
        "max_posts": {
            "type": UserInput.OPTION_TEXT,
            "help": "Messages per group",
            "min": 1,
            "max": 50000,
            "default": 10
        },
        "daterange": {
            "type": UserInput.OPTION_DATERANGE,
            "help": "Date range"
        },
        "continue-collection": {
            "type": UserInput.OPTION_TOGGLE,
            "help": "Continuous collection",
            "default": False,
            "tooltip": "Setting this option will mean that collection with continue until you actively stop it or the "
                       "max date is passed (set this for far into the future if you don't know when collection will be "
                       "stopped). If the min date is not set to \"today\", messages from prior to today will first be "
                       "collected, then the processor will switch to listening out for new messages and some message "
                       "updates. Note that the \"max items\" field will still apply per channel for existing messages "
                       "(collected prior to today), but will then be used to determine the amount of new messages to "
                       "collect before creating a new results subfile."
        },
        "divider-2": {
            "type": UserInput.OPTION_DIVIDER
        },
        "info-sensitive": {
            "type": UserInput.OPTION_INFO,
            "help": "Your API credentials and phone number **will be sent to the 4CAT server** and will be stored "
                    "there while data is fetched. After the dataset has been created your credentials will be "
                    "deleted from the server, unless you enable the option below. If you want to download images "
                    "attached to the messages in your collected data, you need to enable this option. Your "
                    "credentials will never be visible to other users and can be erased later via the result page."
        },
        "save-session": {
            "type": UserInput.OPTION_TOGGLE,
            "help": "Save session:",
            "default": False
        },
        "info-google-drive": {
            "type": UserInput.OPTION_INFO,
            "help": "This option allows you to save the results of this search to your own google drive in a folder "
                    "named fourcat-auto/. In order to use this option, you *must* ensure you have logged into google"
                    "drive using the \"Login to Google Drive\" option in the header above."
        },
        "save-to-gdrive": {
            "type": UserInput.OPTION_TOGGLE,
            "help": "Save to google drive:",
            "default": False
        },
        "resolve-entities-intro": {
            "type": UserInput.OPTION_INFO,
            "help": "4CAT can resolve the references to channels and user and replace the numeric ID with the full "
                    "user, channel or group metadata. Doing so allows one to discover e.g. new relevant groups and "
                    "figure out where or who a message was forwarded from. However, this increases query time and "
                    "for large datasets, increases the chance you will be rate-limited and your dataset isn't able "
                    "to finish capturing. It will also dramatically increase the disk space needed to store the "
                    "data, so only enable this if you really need it!"
        },
        "resolve-entities": {
            "type": UserInput.OPTION_TOGGLE,
            "help": "Resolve references",
            "default": False,
        },
        "include-actions-intro": {
            "type": UserInput.OPTION_INFO,
            "help": "4CAT can include actions which are not necessarily messages as part of the dataset. These occur "
                    "whenever: a new chat is created, a chat’s title or photo is changed or removed, a new message is "
                    "pinned, a user scores in a game, a user joins or is added to the group or a user is removed or "
                    "leaves a group should it have less than 50 members or the removed user was a bot."
        },
        "include-actions": {
            "type": UserInput.OPTION_TOGGLE,
            "help": "Include actions",
            "default": False,
        },
        "retrieve-replies-intro": {
            "type": UserInput.OPTION_INFO,
            "help": "Some public channels have linked discussion groups where users can comment/reply to posts made "
                    "in the original channel. Enabling this option allows you to check if a channel has a publicly "
                    "available linked discussion group and collect data from here too. Note that replies are collected "
                    "with the same parameters (dates, number of messages to collect) as the main channel. Enabling this "
                    "will increase the size of your dataset."
        },
        "retrieve-replies": {
            "type": UserInput.OPTION_TOGGLE,
            "help": "Retrieve replies",
            "default": False,
        }
    }

    def get_items(self, query):
        """
        Execute a query; get messages for given parameters

        Basically a wrapper around execute_queries() to call it with asyncio.

        :param dict query:  Query parameters, as part of the DataSet object
        :return list:  Posts, sorted by thread and post ID, in ascending order
        """
        if "api_phone" not in query or "api_hash" not in query or "api_id" not in query:
            self.dataset.update_status("Could not create dataset since the Telegram API Hash and ID are missing. Try "
                                       "creating it again from scratch.", is_final=True)
            return None

        self.details_cache = {}
        self.failures_cache = set()
        results = asyncio.run(self.execute_queries())

        if not query.get("save-session"):
            self.dataset.delete_parameter("api_hash", instant=True)
            self.dataset.delete_parameter("api_phone", instant=True)
            self.dataset.delete_parameter("api_id", instant=True)

        if not self.flawless:
            self.dataset.update_status("Dataset completed, but some requested entities were unavailable (they may have "
                                       "been private). View the log file for details.", is_final=True)

        return results

    async def execute_queries(self):
        """
        Get messages for queries

        This is basically what would be done in get_items(), except due to
        Telethon's architecture this needs to be called in an async method,
        which is this one.

        :return list:  Collected messages
        """
        # session file has been created earlier, and we can re-use it here in
        # order to avoid having to re-enter the security code
        query = self.parameters

        session_code = query.get("sesson-code", None)
        session_id = SearchTelegram.create_session_id(query["api_phone"], query["api_id"], query["api_hash"])

        if session_code:
            session_id = session_id + session_code

        self.dataset.log('Telegram session id: %s' % session_id)
        session_path = Path(config.get("PATH_ROOT")).joinpath(config.get("PATH_SESSIONS"), session_id + ".session")

        client = None

        try:
            client = TelegramClient(str(session_path), int(query.get("api_id")), query.get("api_hash"),
                                    loop=self.eventloop)
            await client.start(phone=SearchTelegram.cancel_start)
        except RuntimeError:
            # session is no longer useable, delete file so user will be asked
            # for security code again. The RuntimeError is raised by
            # `cancel_start()`
            self.dataset.update_status(
                "Session is not authenticated: login security code may have expired. You need to re-enter the security code.",
                is_final=True)

            if client and hasattr(client, "disconnect"):
                await client.disconnect()

            if session_path.exists():
                session_path.unlink()

            return []
        except Exception as e:
            # not sure what exception specifically is triggered here, but it
            # always means the connection failed
            self.log.error("Telegram: %s\n%s" % (str(e), traceback.format_exc()))
            self.dataset.update_status("Error connecting to the Telegram API with provided credentials.", is_final=True)
            if client and hasattr(client, "disconnect"):
                await client.disconnect()
            return []

        # ready our parameters
        parameters = self.dataset.get_parameters()
        queries = [query.strip() for query in parameters.get("query", "").split(",")]
        max_items = convert_to_int(parameters.get("items", 10), 10)
        continue_collection = parameters.get("continue-collection")
        save_to_gdrive = parameters.get("save-to-gdrive")
        drive_client = None

        # set up google drive client if we're going to need it
        if save_to_gdrive:
            credentials = self.dataset.get_owner_drive_credentials()
            if credentials:
                drive_client = build('drive', 'v3', credentials=credentials)
            else:
                self.dataset.update_status("You have selected \"Save to Google Drive\", but  4cat cannot access your "
                                           "drive. Use the \"Login to Google Drive\" button above before starting a new "
                                           "collection")
                return []

        # Telethon requires the offset date to be a datetime date
        max_date = parameters.get("max_date")
        if max_date:
            try:
                max_date = datetime.fromtimestamp(int(max_date))
                max_date = max_date.replace(hour=23, minute=59, second=59)
            except ValueError:
                max_date = None

        # min_date can remain an integer
        min_date = parameters.get("min_date")
        if min_date:
            try:
                min_date = int(min_date)
            except ValueError:
                min_date = None

        posts = []
        try:
            async for post in self.gather_posts(client, queries, max_items, min_date, max_date):
                posts.append(post)

            # update or initial posts come in reverse order.
            # can be changed at client collection point, keeping it here for one useful and one workaround reason
            posts.reverse()
            initial_file = False if self.dataset.get_last_update_markers() else True
            await self.save_files(posts, drive_client, initial_file=initial_file)

            if continue_collection:
                self.dataset.mark_continuous()
                cont_posts = await self.continuous_collection(client, queries, max_date, max_items, drive_client)
                self.dataset.update_status("Stopping ongoing collection due to user request.")
                if len(cont_posts):
                    posts = cont_posts

            return posts

        except ProcessorInterruptedException as e:
            raise e
        except Exception as e:
            # catch-all so we can disconnect properly
            # ...should we?
            self.dataset.update_status("Error scraping posts from Telegram %s" % e)
            self.log.error("Telegram scraping error: %s" % traceback.format_exc())
            return []
        finally:
            await client.disconnect()

    async def gather_posts(self, client, queries, max_items, min_date, max_date):
        """
        Gather messages for each entity for which messages are requested

        :param TelegramClient client:  Telegram Client
        :param list queries:  List of entities to query (as string)
        :param int max_items:  Messages to scrape per entity
        :param int min_date:  Datetime date to get posts after
        :param int max_date:  Datetime date to get posts before
        :return list:  List of messages, each message a dictionary.
        """
        resolve_refs = self.parameters.get("resolve-entities")
        include_actions = self.parameters.get("include-actions")
        retrieve_replies = self.parameters.get("retrieve-replies")

        # Adding flag to stop; using for rate limits
        no_additional_queries = False

        # Collect queries
        processed = 0
        for query in queries:
            delay = 10
            min_id = 0
            retries = 0
            processed += 1
            reply_channel_added = False
            self.dataset.update_progress(processed / len(queries))

            if no_additional_queries:
                # Note that we are note completing this query
                self.dataset.update_status("Rate-limited by Telegram; not executing query %s" % query)
                continue

            while True:
                self.dataset.update_status("Fetching messages for entity '%s'" % query)
                i = 0
                try:
                    entity_posts = 0

                    # if chat channels are added, they are id-ed using numeric id
                    # this will fail if they are then formatted as a string on input
                    try:
                        query = int(query)
                    except ValueError:
                        pass

                    if self.dataset.is_continuous() and self.dataset.get_last_update_markers():

                        self.dataset.update_status("It looks like this is a continuous collector which has been "
                                                   "restarted, so only retrieving missing posts since then.")

                        markers = self.dataset.get_last_update_markers()
                        query = await client.get_peer_id(query)

                        if query and str(query) in markers.keys():
                            min_id = markers[str(query)]

                    async for message in client.iter_messages(entity=query, offset_date=max_date, min_id=min_id):
                        entity_posts += 1
                        i += 1
                        if self.interrupted:
                            raise ProcessorInterruptedException(
                                "Interrupted while fetching message data from the Telegram API")

                        if entity_posts % 100 == 0:
                            self.dataset.update_status(
                                "Retrieved %i posts for entity '%s' (%i total)" % (entity_posts, query, i))

                        if (not include_actions) and (message.action is not None):
                            # e.g. someone joins the channel - not an actual message
                            continue

                        if retrieve_replies and (not reply_channel_added) and (message.replies and
                                                                               message.replies.channel_id):
                            listed_reply_channel = str(message.replies.channel_id)

                            self.dataset.update_status("Reply channel '%s' found and added to process queue"
                                                       % listed_reply_channel)

                            channel_to_add = utils.get_peer_id(message.replies.channel_id)
                            # making sure this is cached in the session file so it's actually found
                            to_cache = await client.get_entity(channel_to_add)
                            queries.append(channel_to_add)
                            reply_channel_added = True

                        # todo: possibly enrich object with e.g. the name of
                        # the channel a message was forwarded from (but that
                        # needs extra API requests...)
                        serialized_message = SearchTelegram.serialize_obj(message)
                        if resolve_refs:
                            serialized_message = await self.resolve_groups(client, serialized_message)

                        # Stop if we're below the min date
                        if min_date and serialized_message.get("date") < min_date:
                            break

                        yield serialized_message

                        if entity_posts >= max_items:
                            break

                except ChannelPrivateError:
                    self.dataset.update_status("Entity %s is private, skipping" % query)
                    self.flawless = False

                except (UsernameInvalidError,):
                    self.dataset.update_status("Could not scrape entity '%s', does not seem to exist, skipping" % query)
                    self.flawless = False

                except FloodWaitError as e:
                    self.dataset.update_status("Rate-limited by Telegram: %s; waiting" % str(e))
                    if e.seconds < self.end_if_rate_limited:
                        time.sleep(e.seconds)
                        continue
                    else:
                        self.flawless = False
                        no_additional_queries = True
                        self.dataset.update_status(
                            "Telegram wait grown large than %i minutes, ending" % int(e.seconds / 60))
                        break

                except BadRequestError as e:
                    self.dataset.update_status(
                        "Error '%s' while collecting entity %s, skipping" % (e.__class__.__name__, query))
                    self.flawless = False

                except ValueError as e:
                    self.dataset.update_status("Error '%s' while collecting entity %s, skipping" % (str(e), query))
                    self.flawless = False

                except ChannelPrivateError as e:
                    self.dataset.update_status(
                        "QUERY '%s' unable to complete due to error %s. Skipping." % (
                            query, str(e)))
                    break

                except TimeoutError:
                    if retries < 3:
                        self.dataset.update_status(
                            "Tried to fetch messages for entity '%s' but timed out %i times. Skipping." % (
                                query, retries))
                        self.flawless = False
                        break

                    self.dataset.update_status(
                        "Got a timeout from Telegram while fetching messages for entity '%s'. Trying again in %i seconds." % (
                            query, delay))
                    time.sleep(delay)
                    delay *= 2
                    continue

                break

    async def resolve_groups(self, client, message):
        """
        Recursively resolve references to groups and users

        :param client:  Telethon client instance
        :param dict message:  Message, as already mapped by serialize_obj
        :return:  Resolved dictionary
        """
        resolved_message = message.copy()
        for key, value in message.items():
            try:
                if type(value) is not dict:
                    # if it's not a dict, we never have to resolve it, as it
                    # does not represent an entity
                    continue

                elif "_type" in value and value["_type"] in ("InputPeerChannel", "PeerChannel"):
                    # forwarded from a channel!
                    if value["channel_id"] in self.failures_cache:
                        continue

                    if value["channel_id"] not in self.details_cache:
                        channel = await client(GetFullChannelRequest(value["channel_id"]))
                        self.details_cache[value["channel_id"]] = SearchTelegram.serialize_obj(channel)

                    resolved_message[key] = self.details_cache[value["channel_id"]]
                    resolved_message[key]["channel_id"] = value["channel_id"]

                elif "_type" in value and value["_type"] == "PeerUser":
                    # a user!
                    if value["user_id"] in self.failures_cache:
                        continue

                    if value["user_id"] not in self.details_cache:
                        user = await client(GetFullUserRequest(value["user_id"]))
                        self.details_cache[value["user_id"]] = SearchTelegram.serialize_obj(user)

                    resolved_message[key] = self.details_cache[value["user_id"]]
                else:
                    resolved_message[key] = await self.resolve_groups(client, value)

            except (TypeError, ChannelPrivateError, UsernameInvalidError) as e:
                self.failures_cache.add(value.get("channel_id", value.get("user_id")))
                if type(e) in (ChannelPrivateError, UsernameInvalidError):
                    self.dataset.log("Cannot resolve entity with ID %s of type %s (%s), leaving as-is" % (
                        str(value.get("channel_id", value.get("user_id"))), value["_type"], e.__class__.__name__))
                else:
                    self.dataset.log("Cannot resolve entity with ID %s of type %s, leaving as-is" % (
                    str(value.get("channel_id", value.get("user_id"))), value["_type"]))

        return resolved_message

    @staticmethod
    def cancel_start():
        """
        Replace interactive phone number input in Telethon

        By default, if Telethon cannot use the given session file to
        authenticate, it will interactively prompt the user for a phone
        number on the command line. That is not useful here, so instead
        raise a RuntimeError. This will be caught below and the user will
        be told they need to re-authenticate via 4CAT.
        """
        raise RuntimeError("Connection cancelled")

    @staticmethod
    def map_item(message):
        """
        Convert Message object to 4CAT-ready data object

        :param Message message:  Message to parse
        :return dict:  4CAT-compatible item object
        """

        # todo: not sure why this happens, quick fix for now
        if message["_chat"] is None:
            thread = "error-no-chat"
            thread_num_id = "error-no-id"
        else:
            if message["_chat"]["username"]:
                # chats can apparently not have usernames???
                # truly telegram objects are way too lenient for their own good
                thread = message["_chat"]["username"]
            elif message["_chat"]["title"]:
                thread = re.sub(r"\s", "", message["_chat"]["title"])
            else:
                # just give up
                thread = "unknown"

            if message["_chat"]["id"]:
                thread_num_id = message["_chat"]["id"]
            else:
                # just give up
                thread_num_id = "unknown"

        # determine username
        # API responses only include the user *ID*, not the username, and to
        # complicate things further not everyone is a user and not everyone
        # has a username. If no username is available, try the first and
        # last name someone has supplied
        fullname = ""
        username = ""
        user_id = message["_sender"]["id"] if message.get("_sender") else ""
        user_is_bot = message["_sender"].get("bot", False) if message.get("_sender") else ""

        if message.get("_sender") and message["_sender"].get("username"):
            username = message["_sender"]["username"]

        if message.get("_sender") and message["_sender"].get("first_name"):
            fullname += message["_sender"]["first_name"]

        if message.get("_sender") and message["_sender"].get("last_name"):
            fullname += " " + message["_sender"]["last_name"]

        fullname = fullname.strip()

        # determine media type
        # these store some extra information of the attachment in
        # attachment_data. Since the final result will be serialised as a csv
        # file, we can only store text content. As such some media data is
        # serialised as JSON.
        attachment_type = SearchTelegram.get_media_type(message["media"])
        attachment_filename = ""

        if attachment_type == "contact":
            contact_data = ["phone_number", "first_name", "last_name", "vcard", "user_id"]
            if message["media"].get('contact', False):
                # Old datastructure
                attachment = message["media"]["contact"]
            elif all([property in message["media"].keys() for property in contact_data]):
                # New datastructure 2022/7/25
                attachment = message["media"]
            else:
                raise ProcessorException('Cannot find contact data; Telegram datastructure may have changed')
            attachment_data = json.dumps({property: attachment.get(property) for property in contact_data})

        elif attachment_type == "document":
            # videos, etc
            # This could add a separate routine for videos to make them a
            # separate type, which could then be scraped later, etc
            attachment_type = message["media"]["document"]["mime_type"].split("/")[0]
            if attachment_type == "video":
                attachment = message["media"]["document"]
                attachment_data = json.dumps({
                    "id": attachment["id"],
                    "dc_id": attachment["dc_id"],
                    "file_reference": attachment["file_reference"],
                })
            else:
                attachment_data = ""

        # elif attachment_type in ("geo", "geo_live"):
        # untested whether geo_live is significantly different from geo
        #    attachment_data = "%s %s" % (message["geo"]["lat"], message["geo"]["long"])

        elif attachment_type == "photo":
            # we don't actually store any metadata about the photo, since very
            # little of the metadata attached is of interest. Instead, the
            # actual photos may be downloaded via a processor that is run on the
            # search results
            attachment = message["media"]["photo"]
            attachment_data = json.dumps({
                "id": attachment["id"],
                "dc_id": attachment["dc_id"],
                "file_reference": attachment["file_reference"],
            })
            attachment_filename = thread + "-" + str(message["id"]) + ".jpeg"

        elif attachment_type == "poll":
            # unfortunately poll results are only available when someone has
            # actually voted on the poll - that will usually not be the case,
            # so we store -1 as the vote count
            attachment = message["media"]
            options = {option["option"]: option["text"] for option in attachment["poll"]["answers"]}
            attachment_data = json.dumps({
                "question": attachment["poll"]["question"],
                "voters": attachment["results"]["total_voters"],
                "answers": [{
                    "answer": options[answer["option"]],
                    "votes": answer["voters"]
                } for answer in attachment["results"]["results"]] if attachment["results"]["results"] else [{
                    "answer": options[option],
                    "votes": -1
                } for option in options]
            })

        elif attachment_type == "url":
            # easy!
            attachment_data = message["media"].get("web_preview", {}).get("url", "")

        else:
            attachment_data = ""

        # was the message forwarded from somewhere and if so when?
        forwarded_timestamp = ""
        forwarded_name = ""
        forwarded_username = ""
        if message.get("fwd_from") and "from_id" in message["fwd_from"] and not (
                type(message["fwd_from"]["from_id"]) is int):
            # forward information is spread out over a lot of places
            # we can identify, in order of usefulness: username, full name,
            # and ID. But not all of these are always available, and not
            # always in the same place either
            forwarded_timestamp = int(message["fwd_from"]["date"])
            from_data = message["fwd_from"]["from_id"]

            if from_data:
                forwarded_from_id = from_data.get("channel_id", from_data.get("user_id", ""))

            if message["fwd_from"].get("from_name"):
                forwarded_name = message["fwd_from"].get("from_name")

            if from_data and from_data.get("from_name"):
                forwarded_name = message["fwd_from"]["from_name"]

            if from_data and ("user" in from_data or "chats" in from_data):
                # 'resolve entities' was enabled for this dataset
                if "user" in from_data:
                    if from_data["user"].get("username"):
                        forwarded_username = from_data["user"]["username"]

                    if from_data["user"].get("first_name"):
                        forwarded_name = from_data["user"]["first_name"]
                    if message["fwd_from"].get("last_name"):
                        forwarded_name += "  " + from_data["user"]["last_name"]

                    forwarded_name = forwarded_name.strip()

                elif "chats" in from_data:
                    channel_id = from_data.get("channel_id")
                    for chat in from_data["chats"]:
                        if chat["id"] == channel_id or channel_id is None:
                            forwarded_username = chat["username"]

        msg = {
            # thread and chat are for some reason kept the same in the original (or similar: the latter doesn't null
            # check. introducing thread_num_id for the actual id of the chat)
            "id": message["id"],
            "thread_num_id": thread_num_id,
            "thread_id": thread,
            "chat":  thread,
            "author": user_id,
            "author_username": username,
            "author_name": fullname,
            "author_is_bot": user_is_bot,
            "body": message["message"],
            "reply_to": message.get("reply_to_msg_id", ""),
            "views": message["views"] if message["views"] else "",
            "timestamp": datetime.fromtimestamp(message["date"]).strftime("%Y-%m-%d %H:%M:%S"),
            "unix_timestamp": int(message["date"]),
            "timestamp_edited": datetime.fromtimestamp(message["edit_date"]).strftime("%Y-%m-%d %H:%M:%S") if message[
                "edit_date"] else "",
            "unix_timestamp_edited": int(message["edit_date"]) if message["edit_date"] else "",
            "author_forwarded_from_name": forwarded_name,
            "author_forwarded_from_username": forwarded_username,
            "timestamp_forwarded_from": datetime.fromtimestamp(forwarded_timestamp).strftime(
                "%Y-%m-%d %H:%M:%S") if forwarded_timestamp else "",
            "unix_timestamp_forwarded_from": forwarded_timestamp,
            "attachment_type": attachment_type,
            "attachment_data": attachment_data,
            "attachment_filename": attachment_filename
        }

        return msg

    @staticmethod
    def get_media_type(media):
        """
        Get media type for a Telegram attachment

        :param media:  Media object
        :return str:  Textual identifier of the media type
        """
        try:
            return {
                "NoneType": "",
                "MessageMediaContact": "contact",
                "MessageMediaDocument": "document",
                "MessageMediaEmpty": "",
                "MessageMediaGame": "game",
                "MessageMediaGeo": "geo",
                "MessageMediaGeoLive": "geo_live",
                "MessageMediaInvoice": "invoice",
                "MessageMediaPhoto": "photo",
                "MessageMediaPoll": "poll",
                "MessageMediaUnsupported": "unsupported",
                "MessageMediaVenue": "venue",
                "MessageMediaWebPage": "url"
            }[media.get("_type", None)]
        except (AttributeError, KeyError):
            return ""

    @staticmethod
    def serialize_obj(input_obj):
        """
        Serialize an object as a dictionary

        Telethon message objects are not serializable by themselves, but most
        relevant attributes are simply struct classes. This function replaces
        those that are not with placeholders and then returns a dictionary that
        can be serialized as JSON.

        :param obj:  Object to serialize
        :return:  Serialized object
        """
        scalars = (int, str, float, list, tuple, set, bool)

        if type(input_obj) in scalars or input_obj is None:
            return input_obj

        if type(input_obj) is not dict:
            obj = input_obj.__dict__
        else:
            obj = input_obj.copy()

        mapped_obj = {}
        for item, value in obj.items():
            if type(value) is datetime:
                mapped_obj[item] = value.timestamp()
            elif type(value).__module__ in ("telethon.tl.types", "telethon.tl.custom.forward"):
                mapped_obj[item] = SearchTelegram.serialize_obj(value)
                if type(obj[item]) is not dict:
                    mapped_obj[item]["_type"] = type(value).__name__
            elif type(value) is list:
                mapped_obj[item] = [SearchTelegram.serialize_obj(item) for item in value]
            elif type(value).__module__[0:8] == "telethon":
                # some type of internal telethon struct
                continue
            elif type(value) is bytes:
                mapped_obj[item] = value.hex()
            elif type(value) not in scalars and value is not None:
                # type we can't make sense of here
                continue
            elif type(value) is dict:
                for key, vvalue in value:
                    mapped_obj[item][key] = SearchTelegram.serialize_obj(vvalue)
            else:
                mapped_obj[item] = value

        return mapped_obj

    @staticmethod
    def validate_query(query, request, user):
        """
        Validate Telegram query

        :param dict query:  Query parameters, from client-side.
        :param request:  Flask request
        :param User user:  User object of user who has submitted the query
        :return dict:  Safe query parameters
        """
        # no query 4 u
        if not query.get("query", "").strip():
            raise QueryParametersException("You must provide a search query.")

        if not query.get("api_id", None) or not query.get("api_hash", None) or not query.get("api_phone", None):
            raise QueryParametersException("You need to provide valid Telegram API credentials first.")

        privileged = user.get_value("telegram.can_query_all_messages", False)

        # reformat queries to be a comma-separated list with no wrapping
        # whitespace
        whitespace = re.compile(r"\s+")
        items = whitespace.sub("", query.get("query").replace("\n", ","))
        if len(items.split(",")) > 25 and not privileged:
            raise QueryParametersException("You cannot query more than 25 items at a time.")

        sanitized_items = []
        # handle telegram URLs
        for item in items.split(","):
            if not item.strip():
                continue
            item = re.sub(r"^https?://t\.me/", "", item)
            item = re.sub(r"^/?s/", "", item)
            item = re.sub(r"[/]*$", "", item)
            sanitized_items.append(item)

        # the dates need to make sense as a range to search within
        min_date, max_date = query.get("daterange")

        # now check if there is an active API session
        if not user or not user.is_authenticated or user.is_anonymous:
            raise QueryParametersException("Telegram scraping is only available to logged-in users with personal "
                                           "accounts.")

        # check for the information we need
        session_id = SearchTelegram.create_session_id(query.get("api_phone"), query.get("api_id"),
                                                      query.get("api_hash"))
        session_code = None

        user.set_value("telegram.session", session_id)

        # if there is more than one telegram collector running, should be ensured that only one session
        # is used for each client. since each session requires auth (and setting up too many can easily
        # get you rate limited), pick from previously set up but unused sessions ("tg_codes") where possible.
        # otherwise create a new one. temp_code/temp_session_code" is used to ensure should a new session be
        # created, its id is not overwritten when processing this form for the second time to validate the new session.

        if user.get_telegram_jobs() > 0:
            temp_code = user.get_value("temp_session_code", None)
            old_codes = user.get_value("tg_codes", None)

            if temp_code:
                session_code = temp_code

            elif old_codes:
                usable_old_code = old_codes.split(",")[0]
                updated_old_codes = old_codes.replace(usable_old_code + ",", "")
                user.set_value("tg_codes", updated_old_codes)
                session_code = usable_old_code
                user.set_value("temp_session_code", session_code)

            else:
                session_code = uuid.uuid4().hex[0:4]
                user.set_value("temp_session_code", session_code)

            session_id = session_id + session_code

        session_path = Path(config.get('PATH_ROOT')).joinpath(config.get('PATH_SESSIONS'), session_id + ".session")

        client = None

        # API ID is always a number, if it's not, we can immediately fail
        try:
            api_id = int(query.get("api_id"))
        except ValueError:
            raise QueryParametersException("Invalid API ID.")

        # maybe we've entered a code already and submitted it with the request
        if "option-security-code" in request.form and request.form.get("option-security-code").strip():
            code_callback = lambda: request.form.get("option-security-code")
            max_attempts = 1
        else:
            code_callback = lambda: -1
            # max_attempts = 0 because authing will always fail: we can't wait for
            # the code to be entered interactively, we'll need to do a new request
            # but we can't just immediately return, we still need to call start()
            # to get telegram to send us a code
            max_attempts = 0

        # now try authenticating
        needs_code = False
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            client = TelegramClient(str(session_path), api_id, query.get("api_hash"), loop=loop)

            try:
                client.start(max_attempts=max_attempts, phone=query.get("api_phone"), code_callback=code_callback)

            except ValueError as e:
                # this happens if 2FA is required
                raise QueryParametersException("Your account requires two-factor authentication. 4CAT at this time "
                                               "does not support this authentication mode for Telegram. (%s)" % e)
            except RuntimeError as e:
                # A code was sent to the given phone number
                needs_code = True
        except FloodWaitError as e:
            # uh oh, we got rate-limited
            raise QueryParametersException("You were rate-limited and should wait a while before trying again. " +
                                           str(e).split("(")[0] + ".")
        except ApiIdInvalidError as e:
            # wrong credentials
            raise QueryParametersException("Your API credentials are invalid.")
        except PhoneNumberInvalidError as e:
            # wrong phone number
            raise QueryParametersException(
                "The phone number provided is not a valid phone number for these credentials.")
        except Exception as e:
            # ?
            raise QueryParametersException(
                "An unexpected error (%s) occurred and your authentication could not be verified." % e)
        finally:
            if client:
                client.disconnect()

        if needs_code:
            raise QueryNeedsFurtherInputException(config={
                "code-info": {
                    "type": UserInput.OPTION_INFO,
                    "help": "Please enter the security code that was sent to your Telegram app to continue."
                },
                "security-code": {
                    "type": UserInput.OPTION_TEXT,
                    "help": "Security code",
                    "sensitive": True
                }})

        # simple!
        return {
            "items": query.get("max_posts"),
            "query": ",".join(sanitized_items),
            "board": "",  # needed for web interface
            "api_id": query.get("api_id"),
            "api_hash": query.get("api_hash"),
            "api_phone": query.get("api_phone"),
            "save-session": query.get("save-session"),
            "save-to-gdrive": query.get("save-to-gdrive"),
            "continue-collection": query.get("continue-collection"),
            "resolve-entities": query.get("resolve-entities"),
            "include-actions": query.get("include-actions"),
            "retrieve-replies": query.get("retrieve-replies"),
            "min_date": min_date,
            "max_date": max_date,
            "session-code": session_code
        }

    @staticmethod
    def create_session_id(api_phone, api_id, api_hash):
        """
        Generate a filename for the session file

        This is a combination of phone number and API credentials, but hashed
        so that one cannot actually derive someone's phone number from it.

        :param str api_phone:  Phone number for API ID
        :param int api_id:  Telegram API ID
        :param str api_hash:  Telegram API Hash
        :return str: A hash value derived from the input
        """
        hash_base = api_phone.strip().replace("+", "") + str(api_id).strip() + api_hash.strip()
        return hashlib.blake2b(hash_base.encode("ascii")).hexdigest()

    @classmethod
    def get_options(cls=None, parent_dataset=None, user=None):
        """
        Get processor options

        This method by default returns the class's "options" attribute, but
        will lift the limit on the amount of messages scraped per group if the
        user requesting the options has been configured as such.

        :param DataSet parent_dataset:  An object representing the dataset that
        the processor would be run on
        :param User user:  Flask user the options will be displayed for, in
        case they are requested for display in the 4CAT web interface. This can
        be used to show some options only to privileges users.
        """
        options = cls.options.copy()

        if user and user.get_value("telegram.can_query_all_messages", False):
            if "max" in options["max_posts"]:
                del options["max_posts"]["max"]

            options["query-intro"]["help"] = "You can collect messages from multiple entities (channels or groups). " \
                                             "Separate with commas or line breaks."

        return options

    async def forward_message_handler(self, event, client, cont_posts):
        """
        Handler for telethon forwarded message event: serialize message, add to list of messages

        :param ForwardedMessage event:  Telegram forward message event
        :param TelegramClient client:  Telegram client
        :param list cont_posts:  List of entities to query (as string)
        """

        self.dataset.update_status("Forwarded message found. Retrieving forwarded message")

        try:
            forwarded_message = await client.get_messages(event.channel_id, event.id)
            serialized_message = SearchTelegram.serialize_obj(forwarded_message)
            cont_posts.append(serialized_message)

        except ValueError:
            self.dataset.update_status("Could not find channel or message with ids %s and %s respectively"
                                       % str(event.channel_id), str(event.id))

        self.dataset.update_status("Forwarded message retrieved. Continuing to listen for new messages")

    async def new_message_handler(self, event, cont_posts):
        """
        Handler for telethon new message event: serialize message, add to list of messages

        :param NewMessage event:  Telegram new message event
        :param list cont_posts:  List of entities to query (as string)
        """

        self.dataset.update_status("New message found. Retrieving new message.")

        message = event.message
        serialized_message = SearchTelegram.serialize_obj(message)
        cont_posts.append(serialized_message)

        self.dataset.update_status("Message retrieved. Continuing to listen for new messages...")

    async def continuous_collection(self, client, queries, max_date, max_items, gdrive_client):
        """
        Continuously gather messages for each entity for which messages are requested

        :param TelegramClient client:  Telegram Client
        :param list queries:  List of entities to query (as string)
        :param int max_date:  Datetime date to get posts before
        :param int max_items:  Maximum number of items per file
        :return list:  List of messages, each message a dictionary.
        """

        queries_to_poll = []
        continuous_posts = []
        final_file_posts = []
        delay = 10

        self.dataset.update_status("Checking entities we would like to collect data from exist")

        for query in queries:
            try:
                telegram_entity = await client.get_entity(query)
                if telegram_entity:
                    queries_to_poll.append(query)
            except (ValueError, UsernameNotOccupiedError):
                self.dataset.update_status("Could not poll entity '%s', does not seem to exist, skipping" % query)
                self.flawless = False

        if not queries_to_poll:
            self.dataset.update_status("None of the listed entities could be found. Exiting collection")
            return

        self.dataset.update_status("Adding listeners for new and forwarded messages")

        fmh_partial = functools.partial(self.forward_message_handler, cont_posts=continuous_posts, client=client)
        nmh_partial = functools.partial(self.new_message_handler, cont_posts=continuous_posts)

        client.add_event_handler(nmh_partial, events.NewMessage(chats=queries_to_poll))
        client.add_event_handler(fmh_partial, ForwardedMessage(chats=queries_to_poll))

        self.dataset.update_status("Listening for new messages")

        while True:
            try:
                # https://stackoverflow.com/questions/61022878/how-to-run-a-thread-alongside-telethon-client
                await asyncio.sleep(0.1)

                # todo: better approach maybe needed
                now = datetime.now()
                is_eod = now.hour == 23 and now.minute == 59 and now.second == 59

                if (len(continuous_posts) >= max_items) or (is_eod and len(continuous_posts)):
                    await self.save_files(continuous_posts, gdrive_client)
                    final_file_posts = final_file_posts + continuous_posts
                    continuous_posts.clear()

                if self.interrupted == self.INTERRUPT_STOP:

                    if len(continuous_posts):
                        self.dataset.update_status("Saving latest messages before stopping collection")
                        await self.save_files(continuous_posts, gdrive_client)
                        final_file_posts = final_file_posts + continuous_posts

                    await self.save_to_zip_file()
                    self.interrupted = False
                    break

                if max_date and datetime.now() > max_date:
                    self.dataset.update_status("Stopping ongoing collection due to requested max date: %s"
                                               % str(max_date))
                    break

                elif self.interrupted:
                    raise ProcessorInterruptedException(
                        "Interrupted while fetching message data from the Telegram API")

            # not sure if we want these
            # plus, check if they fall within the scope of the client event loop anyway
            except FloodWaitError as e:
                self.dataset.update_status("Rate-limited by Telegram: %s; waiting" % str(e))
                if e.seconds < self.end_if_rate_limited:
                    time.sleep(e.seconds)
                    continue
                else:
                    self.flawless = False
                    self.dataset.update_status(
                        "Telegram wait grown large than %i minutes, ending" % int(e.seconds / 60))
                    break

            except TimeoutError:
                self.dataset.update_status(
                    "Got a timeout from Telegram while fetching messages for entity '%s'. Trying again in %i seconds." % (
                        query, delay))
                time.sleep(delay)
                delay *= 2
                continue

        return final_file_posts

    def update_latest_markers(self, continuous_posts):
        """
        Updates the database to store the id of the last saved post for each entity being collected from
        This is to allow 4Cat to pick up any missing posts should the application restart whilst a continuous
        collection is in progress

        :param continuous_posts: the set of posts which have been saved
        """

        final_message_ids = {} if not self.dataset.get_last_update_markers() else self.dataset.get_last_update_markers()
        continuous_posts.reverse()

        try:
            channels = set([record["_input_chat"]["channel_id"] for record in continuous_posts])

            for channel in channels:
                record = next(post for post in continuous_posts if post["_input_chat"]["channel_id"] == channel)

                if record:
                    message_id = record["id"]
                    channel_id = str(utils.get_peer_id(channel))

                    if channel_id in final_message_ids.keys():
                        final_message_ids.update({channel_id: message_id})
                    else:
                        final_message_ids[channel_id] = message_id

            self.dataset.update_last_update_markers(final_message_ids)

        except KeyError as e:
            self.dataset.log("Error: %s" % e)
            self.dataset.update_status("It looks like we are unable to retrive either the message of channel id")


    async def save_files(self, items, gdrive_client, initial_file=False):
        """
        Save subfile to dataset

        This method saves a given set of data as a subfile to the dataset. It increases
        the number of files a dataset has by one, then creates a file with the main
        dataset result path name, suffixed with the new number of files. In then saves
        all given items to this dataset "subfile"

        :params List items: items to save in subfile
        """

        results_file = self.dataset.get_results_path()
        timestr = time.strftime("%Y%m%d_%H%M%S")
        subfile_name = str(self.dataset.get_initial_filepath().name) if initial_file else \
            str(results_file.stem) + "-" + timestr + str(results_file.suffix)

        subfile_path = Path(str(results_file.parent) + "/" + subfile_name)

        if items:
            self.dataset.update_status("Writing currently collected data to dataset file")
            if results_file.suffix == ".ndjson":
                self.items_to_ndjson(items, subfile_path)
                record = self.dataset.add_subfile_record(subfile_name, "ndjson")
                if gdrive_client:
                    await self.save_to_google_drive(gdrive_client, subfile_path, record, "application/x-ndjson")

            elif results_file.suffix == ".csv":
                self.items_to_csv(items, subfile_path)
                record = self.dataset.add_subfile_record(subfile_name, "csv")
                if gdrive_client:
                    await self.save_to_google_drive(gdrive_client, subfile_path,  record, "text/csv")
            else:
                raise NotImplementedError("Datasource query cannot be saved as %s file" % results_file.suffix)

        self.update_latest_markers(items)

    async def save_to_zip_file(self):
        """
        Save subfiles to zipfile

        Takes all dataset subfiles and zips them into a directory
        This method should probably be moved to the worker
        """
        zip_obj = ZipFile(str(self.dataset.get_results_path()) + ".zip", 'w')

        for i in self.dataset.get_subfile_paths():
            zip_obj.write(i, arcname=str(i.name))

        zip_obj.close()

    async def save_to_google_drive(self, gdrive_client, file_path, record, mime_type):
        """
       Save given file to google drive

       :params gdrive_client: client to interact with google drive
       :params file_path: path of file to upload
       :params record: the database record linked to this file
       :params mime_type: mime type of file to upload
       """

        self.dataset.update_status("Attempting to write subfile to google drive")

        try:
            drive_dir_id = self.dataset.get_drive_dir_id()
            path = str(file_path)

            filename = path.split("/")[-1]

            body = {'name': filename, 'mimeType': mime_type, 'parents': [drive_dir_id]}
            file_data = open(path, 'rb')

            media_body = MediaIoBaseUpload(file_data, mimetype=mime_type, resumable=True)
            gdrive_client.files().create(body=body, media_body=media_body,
                                         fields='id,name,mimeType,createdTime,modifiedTime').execute()

            record.change_uploaded_date(int(time.time()))

            self.dataset.update_status("Finished writing subfile to google drive")

        #catch all for now, finesse later
        except Exception as e:
            self.dataset.update_status("Failed to write file %s to google drive. Ignoring, and continuing "
                                       "with collection. " % (str(file_path.name)))
            self.log.error("Telegram: %s\n%s" % (str(e), traceback.format_exc()))
            return

    def after_process(self):
        """
        Override of the same function in processor.py
        Used to set the initial file to the main file
        """
        super().after_process()

        # if there's an initial file, set this to the results file
        initial_file = self.dataset.get_initial_filepath()

        if initial_file.exists():
            self.dataset.log("Found an initial file. Rewriting the results file to be this file.")
            initial_file.rename(self.dataset.get_results_path())
            self.dataset.remove_initial_record(initial_file.name)


