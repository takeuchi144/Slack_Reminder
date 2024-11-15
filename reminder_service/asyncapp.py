import os
import re
import logging
from datetime import datetime
from typing import Dict, List, Any
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.aws_lambda import SlackRequestHandler
import asyncio
import pytz
import json
import logging
import boto3
from botocore.exceptions import ClientError

class ReminderApp:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ReminderApp, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance

    def initialize(self):
        self.logger = self.setup_logger()
        self.load_env()
        self.apps: Dict[str, AsyncApp] = {}
        self.schedule_channel_id = None
        self.reminder_channel_id = None
        self.team_id = None
        self.dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-west-1'))
        self.test_aws_connection()

    async def get_app(self, team_id: str) -> AsyncApp:
        if team_id not in self.apps:
            token = await self.get_bot_token(team_id)
            self.apps[team_id] = AsyncApp(token=token)
            self.apps[team_id].event("app_mention")(
                ack=self.acknowledge_event,
                lazy=[self.handle_app_mention]
                )
            self.apps[team_id].event("message_changed")(
                ack=self.acknowledge_event,
                lazy=[self.handle_message_changed]
                )
            self.apps[team_id].event("app_installed")(
                ack=self.acknowledge_event,
                lazy=[self.handle_app_installed]
                )
            self.apps[team_id].event("team_join")(
                ack=self.acknowledge_event,
                lazy=[self.handle_team_join]
                )
        return self.apps[team_id]

    def load_env(self):
        try:
            with open('env.json', 'r') as f:
                self.env_vars = json.load(f)
            for key, value in self.env_vars.items():
                os.environ[key] = value
        except FileNotFoundError:
            self.logger.info("env.jsonファイルが見つかりません。AWS Systems Managerからパラメータを取得します。")
            ssm = boto3.client('ssm')
            paginator = ssm.get_paginator('get_parameters_by_path')
            for page in paginator.paginate(Path='/slack', Recursive=True, WithDecryption=True):
                for param in page['Parameters']:
                    key = param['Name'].split('/')[-1]
                    os.environ[key] = param['Value']
                    self.logger.info(f"key:{key}:value{param['Value']}")
            self.logger.info("AWS Systems Managerからパラメータを環境変数に設定しました。")
        except json.JSONDecodeError:
            self.logger.error("env.jsonファイルの解析に失敗しました。")

    def save_env(self):
        try:
            with open('env.json', 'w') as f:
                json.dump(self.env_vars, f, indent=2)
        except Exception as e:
            self.logger.error(f"env.jsonファイルの保存に失敗しました: {str(e)}")

    @staticmethod
    def setup_logger():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def test_aws_connection(self):
        try:
            self.dynamodb.meta.client.list_tables()
            self.logger.info("AWSへの接続テストに成功しました。")
        except Exception as e:
            self.logger.error(f"AWSへの接続テストに失敗しました: {str(e)}")
            raise

    async def parse_and_save_reminders(self, text: str, message_ts: str) -> bool:
        table = self.dynamodb.Table('RemindersTable')
        
        date_pattern: str = r'(\d{1,2}/\d{1,2})((?:\s+.*?(?=\n\d{1,2}/\d{1,2}|\Z)))'
        reminders: List[tuple] = re.findall(date_pattern, text, re.DOTALL)
        
        success = True
        changes = []
        for date, content in reminders:
            mention_pattern: str = r'(@\S+(?:\s+\([^)]+\))?)'
            mentions: List[str] = re.findall(mention_pattern, content)
            mentions = ['<' + mention for mention in mentions]
            message: str = re.sub(mention_pattern, '', content).strip()
            
            item: Dict[str, Any] = {
                'team_id': self.team_id,
                'date': date.strip(),
                'users': mentions,
                'message': message,
                'message_ts': message_ts
            }
            existing_items = table.query(
                    KeyConditionExpression='team_id = :team_id AND #date = :date',
                    ExpressionAttributeNames={'#date': 'date'},
                    ExpressionAttributeValues={':team_id': self.team_id, ':date': item['date']}
            )
                
            if existing_items.get('Items'):
                self.logger.info(f"既存のリマインダー: {existing_items['Items']}")
                self.logger.info(f"新規のリマインダー: {item}")
                old_item = existing_items['Items'][0]
                if {k: v for k, v in old_item.items() if k != 'message_ts'} != {k: v for k, v in item.items() if k != 'message_ts'}:
                   
                    changes.append({
                            'type': '更新',
                            'old': old_item,
                            'new': item
                        })
            else:
                changes.append({
                        'type': '新規作成',
                        'new': item
                    })
                
            table.put_item(Item=item)
            self.logger.info(f"リマインダーを保存しました: 日付: {item['date']}, ユーザー: {', '.join(item['users'])}, メッセージ: {item['message']}")
        
        if changes:
            await self.notify_changes(changes, message_ts)
        else:
            self.logger.info("リマインダーの変更はありませんでした。")
        
        return success

    async def notify_changes(self, changes: List[Dict[str, Any]], message_ts: str) -> None:
        change_messages = []
        for change in changes:
            if change['type'] == '更新':
                old, new = change['old'], change['new']
                change_messages.append(f"更新:\n"
                                       f"日付: {new['date']}\n"
                                       f"ユーザー: {', '.join(old['users'])} → {', '.join(new['users'])}\n"
                                       f"メッセージ: {old['message']} → {new['message']}")
            else:
                new = change['new']
                change_messages.append(f"新規作成:\n"
                                       f"日付: {new['date']}\n"
                                       f"ユーザー: {', '.join(new['users'])}\n"
                                       f"メッセージ: {new['message']}")
        
        change_message = "\n\n".join(change_messages)
        self.logger.info(f"リマインダーの変更:\n{change_message}")
        
        app = self.get_app(self.team_id)
        await app.client.chat_postMessage(
            channel=self.schedule_channel_id,
            text=f"リマインダーが更新されました。変更内容:\n{change_message}",
            thread_ts=message_ts
        )

    @staticmethod
    async def acknowledge_event(body: Dict[str, Any], ack: callable) -> None:
        await ack("リクエストを受け取り、処理中です。")

    async def get_or_create_channel(self, channel_name: str) -> str:
        channel_id = getattr(self, f"{channel_name}_channel_id")
        if channel_id:
            return channel_id

        app = await self.get_app(self.team_id)
        channels = await app.client.conversations_list()
        for channel in channels["channels"]:
            if channel["name"] == channel_name:
                setattr(self, f"{channel_name}_channel_id", channel["id"])
                return channel["id"]
        
        new_channel = await app.client.conversations_create(name=channel_name)
        channel_id = new_channel["channel"]["id"]
        setattr(self, f"{channel_name}_channel_id", channel_id)
        self.logger.info(f"新しい{channel_name}チャンネルを作成しました: {channel_id}")

        users = await app.client.users_list()
        for user in users["members"]:
            if not user["is_bot"] and not user["is_app_user"]:
                try:
                    await app.client.conversations_invite(
                        channel=channel_id,
                        users=user["id"]
                    )
                except Exception as e:
                    self.logger.error(f"ユーザー {user['id']} を{channel_name}チャンネルに追加できませんでした: {str(e)}")

        return channel_id

    async def get_or_create_schedule_channel(self) -> str:
        return await self.get_or_create_channel("schedule")

    async def get_or_create_reminder_channel(self) -> str:
        return await self.get_or_create_channel("reminder")

    async def handle_app_mention(self, body: Dict[str, Any], say: callable) -> None:
        self.logger.info(f"受信したメンション: {body}")
        event: Dict[str, Any] = body["event"]
        channel: str = event["channel"]
        text: str = event["text"]
        message_ts: str = event["ts"]
        self.team_id = body["team_id"]

        if "test" in text.lower():
            await self.send_reminders(body, None)
            await say("テストリマインダーを送信しました。")
            return

        schedule_channel = await self.get_or_create_schedule_channel()
        if channel == schedule_channel:
            success = await self.parse_and_save_reminders(text, message_ts)
            app = await self.get_app(self.team_id)
            if success:
                await app.client.chat_postMessage(
                    channel=channel,
                    text="リマインダーの設定が完了しました。",
                    thread_ts=message_ts
                )
                self.logger.info(f"リマインダーの設定が完了しました: {message_ts}")
            else:
                await app.client.chat_postMessage(
                    channel=channel,
                    text="リマインダーの設定に失敗しました。正しい形式で入力されているか確認してください。",
                    thread_ts=message_ts
                )
                self.logger.error(f"リマインダーの設定に失敗しました: {message_ts}")
        else:
            self.logger.info(f"スケジュールチャンネルではなかったため、リマインドは更新されませんでした: {message_ts}")

    async def handle_message_changed(self, body: Dict[str, Any], say: callable) -> None:
        self.logger.info(f"受信したメッセージ変更: {body}")
        event: Dict[str, Any] = body["event"]
        channel: str = event["channel"]
        message: Dict[str, Any] = event["message"]
        text: str = message["text"]
        message_ts: str = message["ts"]
        self.team_id = body["team_id"]

        schedule_channel = await self.get_or_create_schedule_channel()
        if channel == schedule_channel and message.get("bot_id") is None:
            success = await self.parse_and_save_reminders(text, message_ts)
            app = await self.get_app(self.team_id)
            if success:
                self.logger.info(f"リマインダーが更新されました: {message_ts}")
            else:
                await app.client.chat_postMessage(
                    channel=channel,
                    text="リマインダーの更新に失敗しました。正しい形式で入力されているか確認してください。",
                    thread_ts=message_ts
                )
                self.logger.error(f"リマインダーの更新に失敗しました: {message_ts}")

    async def send_reminders(self, event: Dict[str, Any], context: Any) -> None:
        self.logger.info(f"受信したイベント: {event}")
        jst: pytz.timezone = pytz.timezone('Asia/Tokyo')
        now: datetime = datetime.now(jst)
        today: str = now.strftime("%m/%d")

        table = self.dynamodb.Table('RemindersTable')
        try:
            response: Dict[str, Any] = table.scan(
                FilterExpression='#date = :today',
                ExpressionAttributeNames={'#date': 'date'},
                ExpressionAttributeValues={':today': today}
            )
        except ClientError as e:
            self.logger.error(f"リマインダーの取得に失敗しました: {e}")
            return

        reminders_by_team = {}
        for item in response.get('Items', []):
            team_id = item['team_id']
            if team_id not in reminders_by_team:
                reminders_by_team[team_id] = []
            reminders_by_team[team_id].append(item)

        for team_id, reminders in reminders_by_team.items():
            self.team_id = team_id
            app = await self.get_app(team_id)
            reminder_channel = await self.get_or_create_reminder_channel()
            for item in reminders:
                users: List[str] = item['users']
                message: str = item['message']
                mentions: str = ' '.join([f"{user}" for user in users])
                await app.client.chat_postMessage(
                    channel=reminder_channel,
                    text=f"{mentions} リマインダー: {message}"
                )
                self.logger.info(f"リマインダーを送信しました: {item}")

    async def get_bot_token(self, team_id: str) -> str:
        ssm = boto3.client('ssm')
        parameter_name = f"/slackapp/SLACK_BOT_TOKEN/{team_id}"
        try:
            response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
            return response['Parameter']['Value']
        except ssm.exceptions.ParameterNotFound:
            self.logger.info(f"パラメータ {parameter_name} が見つかりません。デフォルトのSLACK_BOT_TOKENを使用します。")
            return os.environ[team_id]
        except Exception as e:
            self.logger.error(f"パラメータの取得中にエラーが発生しました: {str(e)}")
            return os.environ[team_id]

    async def handle_app_installed(self, event: Dict[str, Any], client: Any) -> None:
        self.logger.info(f"アプリがインストールされました: {event}")
        self.team_id = event["team_id"]
        await self.get_or_create_schedule_channel()
        await self.get_or_create_reminder_channel()
        self.logger.info("リマインダーチャンネルが作成され、全メンバーが追加されました。")

    async def handle_team_join(self, event: Dict[str, Any], client: Any) -> None:
        self.logger.info(f"新しいメンバーがワークスペースに参加しました: {event}")
        user_id = event["user"]["id"]
        self.team_id = event["team_id"]
        schedule_channel = await self.get_or_create_schedule_channel()
        try:
            await self.app.client.conversations_invite(
                channel=schedule_channel,
                users=user_id
            )
            self.logger.info(f"新しいメンバー {user_id} をリマインダーチャンネルに招待しました。")
        except Exception as e:
            self.logger.error(f"新しいメンバー {user_id} をリマインダーチャンネルに招待できませんでした: {str(e)}")



def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    reminder_app = ReminderApp()
    reminder_app.logger.info(f"受信したイベント: {event}")
    if 'body' in event:
        body = json.loads(event['body'])
        if 'type' in body and body['type'] == 'url_verification':
            return {
                'statusCode': 200,
                'body': json.dumps({'challenge': body['challenge']})
            }
        team_id = body.get('team_id')

        app = asyncio.run(reminder_app.get_app(team_id))
        slack_handler: SlackRequestHandler = SlackRequestHandler(app=app)
        return slack_handler.handle(event, context)
    else:
        asyncio.run(reminder_app.send_reminders(event, context))
        for team_id in set(item['team_id'] for item in event.get('Items', [])):
            reminder_app.team_id = team_id
    

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "リマインダーが正常に処理されました",
            }
        ),
    }

# イベントごとの処理を明記


