import praw

from lib.reddit import init_api_access

if __name__ == "__main__":
    api = init_api_access()
    inbox = api.inbox.all()
    direct_messages = [
        item for item in inbox if isinstance(item, praw.models.Message)
    ]

    for message in direct_messages:
        print(f"From: {message.author.name}")
        print(f"Subject: {message.subject}")
        print(f"Body: {message.body}")
        print("-----")
