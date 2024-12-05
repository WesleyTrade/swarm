import os
import openai
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file
load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')

def test_chat_completion():
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",  # Use "gpt-3.5-turbo" if you don't have access to GPT-4
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello, how are you?"}
            ],
            max_tokens=50,
            temperature=0.7,
        )
        reply = response.choices[0].message['content'].strip()
        logging.info(f"GPT-4 Reply: {reply}")
    except Exception as e:
        logging.error(f"Error in ChatCompletion: {e}")

if __name__ == '__main__':
    test_chat_completion()
