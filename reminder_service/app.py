import os
import re
import logging
import requests
from datetime import datetime
from typing import Dict, List, Any
from slack_bolt import App
from slack_bolt.adapter.aws_lambda import SlackRequestHandler
import pytz
import json
import logging
import boto3
from botocore.exceptions import ClientError
import hashlib
import time

def exchange_code_for_token(code: str) -> str:
    """ 認証コードを使ってSlack APIからアクセストークンを取得する """
    client_id = os.environ['SLACK_CLIENT_ID']
    client_secret = os.environ['SLACK_CLIENT_SECRET'] 
    redirect_uri = os.environ['SLACK_REDIRECT_URI']

    response = requests.post(
        "https://slack.com/api/oauth.v2.access",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri
        }
    )

    data = response.json()
    if data.get("ok"):
        access_token = data["access_token"]
        team_id = data["team"]["id"]
        save_access_token(team_id, access_token)
        return access_token
    else:
        raise Exception("アクセストークンの取得に失敗しました: " + data.get("error", "Unknown error"))

def save_access_token(team_id: str, access_token: str) -> None:
    """取得したアクセストークンをDynamoDBに保存"""
    table = boto3.resource('dynamodb').Table('ReminderSlackAccessTokens')
    table.put_item(Item={"team_id": team_id, "access_token": access_token})

class ReminderApp:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ReminderApp, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance

    def initialize(self):
        self.logger = self.setup_logger()
        #self.load_env()
        self.apps: Dict[str, App] = {}
        self.schedule_channel_id = None
        self.reminder_channel_id = None
        self.team_id = None
        self.dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-west-1'))
        self.test_aws_connection()
        self.processed_requests = set()
    
    def is_duplicate_request(self, request_id: str) -> bool:
        return request_id in self.processed_requests

    def mark_request_as_processed(self, request_id: str) -> None:
        self.processed_requests.add(request_id)

    def get_app(self, team_id: str) -> App:
        if team_id not in self.apps:
            token = self.get_bot_token(team_id)
            self.apps[team_id] = App(token=token)
            self.apps[team_id].event("app_mention")(
                ack=self.acknowledge_event,
                lazy=[self.handle_app_mention]
                )
            self.apps[team_id].event("app_installed")(
                ack=self.acknowledge_event,
                lazy=[self.handle_app_installed]
                )
            self.apps[team_id].event("team_join")(
                ack=self.acknowledge_event,
                lazy=[self.handle_team_join]
                )
            
            # アプリのホーム画面を設定
            self.apps[team_id].event("app_home_opened")(self.update_home_tab)
            
            # アクションの設定
            self.apps[team_id].action("check_schedule")(
                ack=self.acknowledge_event,
                lazy=[self.handle_check_schedule]
            )
        return self.apps[team_id]

    def update_home_tab(self, client, event, logger):
        try:
            client.views_publish(
                user_id=event["user"],
                view={
                    "type": "home",
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "スケジュール管理ボットへようこそ！"
                            }
                        },
                        {
                            "type": "actions",
                            "elements": [
                                {
                                    "type": "button",
                                    "text": {
                                        "type": "plain_text",
                                        "text": "スケジュール確認",
                                        "emoji": True
                                    },
                                    "action_id": "check_schedule"
                                }
                            ]
                        }
                    ]
                }
            )
        except Exception as e:
            logger.error(f"ホーム画面の更新に失敗しました: {e}")

    def handle_check_schedule(self, ack, body, client):
        try:
            user_id = body["user"]["id"]
            
            # DynamoDBからスケジュールを取得
            table = self.dynamodb.Table('RemindersTable')
            response = table.scan()
            schedules = response.get('Items', [])
            
            # DMを開く
            dm = client.conversations_open(users=user_id)
            dm_channel = dm["channel"]["id"]
            
            if not schedules:
                client.chat_postMessage(
                    channel=dm_channel,
                    text="現在設定されているスケジュールはありません。"
                )
                return
            
            # スケジュールの整形と送信
            message = "現在のスケジュール一覧:\n"
            for schedule in schedules:
                users = schedule.get('users', [])
                date = schedule.get('date', '')
                msg = schedule.get('message', '')
                message += f"• {date}: {msg} (対象者: {', '.join(users)})\n"
            
            client.chat_postMessage(
                channel=dm_channel,
                text=message
            )
            
        except Exception as e:
            self.logger.error(f"スケジュール確認中にエラーが発生しました: {e}")


    def get_bot_token(self, team_id: str) -> str:
        """DynamoDBからteam_idに対応するアクセストークンを取得"""
        try:
            table = self.dynamodb.Table('ReminderSlackAccessTokens')
            response = table.get_item(Key={"team_id": team_id})
            return response['Item']['access_token']
        except Exception as e:
            self.logger.error(f"アクセストークンの取得に失敗しました: {str(e)}")
            # フォールバックとしてSSMから取得
            return self._get_token_from_ssm(team_id)

    def _get_token_from_ssm(self, team_id: str) -> str:
        return os.environ["SLACK_BOT_TOKEN"]
        """
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
            return os.environ["SLACK_BOT_TOKEN"]
        """
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

    def parse_and_save_reminders(self, text: str, message_ts: str) -> bool:
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
            self.notify_changes(changes, message_ts)
        else:
            self.logger.info("リマインダーの変更はありませんでした。")
        
        return success

    def notify_changes(self, changes: List[Dict[str, Any]], message_ts: str) -> None:
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
        app.client.chat_postMessage(
            channel=self.schedule_channel_id,
            text=f"リマインダーが更新されました。\n{change_message}",
            thread_ts=message_ts
        )

    @staticmethod
    def acknowledge_event(body: Dict[str, Any], ack: callable) -> None:
        ack("リクエストを受け取り、処理中です。")

    def get_or_create_channel(self, channel_name: str) -> str:
        channel_id = getattr(self, f"{channel_name}_channel_id")
        if channel_id:
            return channel_id

        app = self.get_app(self.team_id)
        channels = app.client.conversations_list()
        for channel in channels["channels"]:
            if channel["name"] == channel_name:
                setattr(self, f"{channel_name}_channel_id", channel["id"])
                return channel["id"]
        
        new_channel = app.client.conversations_create(name=channel_name)
        channel_id = new_channel["channel"]["id"]
        setattr(self, f"{channel_name}_channel_id", channel_id)
        self.logger.info(f"新しい{channel_name}チャンネルを作成しました: {channel_id}")

        users = app.client.users_list()
        for user in users["members"]:
            if not user["is_bot"] and not user["is_app_user"]:
                try:
                    app.client.conversations_invite(
                        channel=channel_id,
                        users=user["id"]
                    )
                except Exception as e:
                    self.logger.error(f"ユーザー {user['id']} を{channel_name}チャンネルに追加できませんでした: {str(e)}")

        return channel_id

    def get_or_create_schedule_channel(self) -> str:
        return self.get_or_create_channel("schedule")

    def get_or_create_reminder_channel(self) -> str:
        return self.get_or_create_channel("reminder")

    def handle_app_mention(self, body: Dict[str, Any], say: callable) -> None:
        self.logger.info(f"受信したメンション: {body}")
        event: Dict[str, Any] = body["event"]
        channel: str = event["channel"]
        text: str = event["text"]
        self.team_id = body["team_id"]
        edited  = event.get("edited")
        message_ts: str = edited["ts"] if edited else event["ts"] 
        if self.is_duplicate_request(message_ts):
            return
        else :
            self.mark_request_as_processed(message_ts)

        if "test" in text.lower():
            self.send_reminders(body, None)
            say("テストリマインダーを送信しました。")
            return

        schedule_channel = self.get_or_create_schedule_channel()
        if channel == schedule_channel:
            success = self.parse_and_save_reminders(text, message_ts)
            app = self.get_app(self.team_id)
            if success:
                if edited:
                    app.client.chat_postMessage(
                    channel=channel,
                    text="メッセージの編集がリマインダーに反映されました。",
                    thread_ts=event["ts"]
                )
                    
                else :
                    app.client.chat_postMessage(
                    channel=channel,
                    text="リマインダーの設定が完了しました。",
                    thread_ts=message_ts
                    )
                #app.client.chat_postMessage(
                #    channel=channel,
                #    text=json.dumps(event, ensure_ascii=False, indent=2),
                #    thread_ts=message_ts)
                self.logger.info(f"リマインダーの設定が完了しました: {message_ts}")
            else:
                app.client.chat_postMessage(
                    channel=channel,
                    text="リマインダーの設定に失敗しました。正しい形式で入力されているか確認してください。",
                    thread_ts=message_ts
                )
                
                self.logger.error(f"リマインダーの設定に失敗しました: {message_ts}")
        else:
            self.logger.info(f"スケジュールチャンネルではなかったため、リマインドは更新されませんでした: {message_ts}")
        
        

    def send_reminders(self, event: Dict[str, Any], context: Any) -> None:
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
            app = self.get_app(team_id)
            reminder_channel = self.get_or_create_reminder_channel()
            for item in reminders:
                users: List[str] = item['users']
                message: str = item['message']
                mentions: str = ' '.join([f"{user}" for user in users])
                app.client.chat_postMessage(
                    channel=reminder_channel,
                    text=f"{mentions} リマインダー: {message}"
                )
                self.logger.info(f"リマインダーを送信しました: {item}")
    


    def handle_app_installed(self, event: Dict[str, Any], client: Any) -> None:
        self.logger.info(f"アプリがインストールされました: {event}")
        self.team_id = event["team_id"]
        schedule_channel = self.get_or_create_schedule_channel()
        reminder_channel = self.get_or_create_reminder_channel()
        
        # ワークスペース内の全ユーザーを取得
        try:
            response = client.users_list()
            for user in response["members"]:
                if not user["is_bot"] and not user["is_app_user"]:
                    user_id = user["id"]
                    # 各ユーザーとDMを開く
                    try:
                        dm_response = client.conversations_open(users=user_id)
                        dm_channel = dm_response["channel"]["id"]
                        client.chat_postMessage(
                            channel=dm_channel,
                            text="はじめまして！私はリマインダーボットです。スケジュールの管理をお手伝いさせていただきます。"
                        )
                        self.logger.info(f"メンバー {user_id} とのDMを開始しました。")
                    except Exception as e:
                        self.logger.error(f"メンバー {user_id} とのDM開始に失敗しました: {str(e)}")
                    
                    # チャンネルに招待
                    try:
                        client.conversations_invite(
                            channel=schedule_channel,
                            users=user_id
                        )
                        client.conversations_invite(
                            channel=reminder_channel, 
                            users=user_id
                        )
                    except Exception as e:
                        self.logger.error(f"メンバー {user_id} のチャンネル招待に失敗しました: {str(e)}")
        except Exception as e:
            self.logger.error(f"ユーザーリストの取得に失敗しました: {str(e)}")

    def handle_team_join(self, event: Dict[str, Any], client: Any) -> None:
        self.logger.info(f"新しいメンバーがワークスペースに参加しました: {event}")
        user_id = event["user"]["id"]
        self.team_id = event["team_id"]
        schedule_channel = self.get_or_create_schedule_channel()
        reminder_channel = self.get_or_create_reminder_channel()

        # 新しいユーザーとのDMを開く
        try:
            response = client.conversations_open(users=user_id)
            dm_channel = response["channel"]["id"]
            client.chat_postMessage(
                channel=dm_channel,
                text="はじめまして！私はリマインダーボットです。スケジュールの管理をお手伝いさせていただきます。"
            )
            self.logger.info(f"新しいメンバー {user_id} とのDMを開始しました。")
        except Exception as e:
            self.logger.error(f"新しいメンバー {user_id} とのDM開始に失敗しました: {str(e)}")

        # チャンネルに招待
        try:
            client.conversations_invite(
                channel=schedule_channel,
                users=user_id
            )
            client.conversations_invite(
                channel=reminder_channel,
                users=user_id
            )
            self.logger.info(f"新しいメンバー {user_id} をチャンネルに招待しました。")
        except Exception as e:
            self.logger.error(f"新しいメンバー {user_id} のチャンネル招待に失敗しました: {str(e)}")



def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    reminder_app = ReminderApp()
    reminder_app.logger.info(f"受信したイベント: {event}")
    
    # CloudWatchからのイベントの場合
    if 'source' in event and event['source'] == 'aws.events':
        reminder_app.send_reminders(event, context)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "CloudWatchからのリマインダーが正常に処理されました",
            })
        }
    
    # OAuth認証のリダイレクトの場合
    query_params = event.get('queryStringParameters')
    if query_params and 'code' in query_params:
        code = query_params['code']
        try:
            access_token = exchange_code_for_token(code)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "認証が完了しました",
                })
            }
        except Exception as e:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": f"認証に失敗しました: {str(e)}",
                })
            }
    
    # Slackからのイベントの場合
    if 'body' in event:
        body = json.loads(event['body'])
        if 'type' in body and body['type'] == 'url_verification':
            return {
                'statusCode': 200,
                'body': json.dumps({'challenge': body['challenge']})
            }
        
        team_id = body.get('team_id')
        app = reminder_app.get_app(team_id)
        slack_handler: SlackRequestHandler = SlackRequestHandler(app=app)
        return slack_handler.handle(event, context)

    return {
        "statusCode": 400,
        "body": json.dumps({
            "message": "不正なイベントフォーマットです",
        })
    }

# イベントごとの処理を明記


