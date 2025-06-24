import asyncio
from bot import main

class FakeContext:
    def __init__(self, body):
        self.req = type("Req", (), {"method": "POST", "path": "/webhook"})()
        self.req_body = body
    def log(self, msg):
        print("[LOG]", msg)
    def error(self, msg):
        print("[ERROR]", msg)
    class res:
        @staticmethod
        def json(obj):
            print("[RESPONSE]", obj)
            return obj

# Пример update из Telegram (теперь с командой /start и реальным chat_id)
test_update = {
    "update_id": 123456789,
    "message": {
        "message_id": 1,
        "from": {"id": 216453, "is_bot": False, "first_name": "Test"},
        "chat": {"id": 216453, "type": "private"},
        "date": 1680000000,
        "text": "/start"
    }
}

if __name__ == "__main__":
    ctx = FakeContext(test_update)
    asyncio.run(main(ctx)) 