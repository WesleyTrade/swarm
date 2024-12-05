import openai
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Print OpenAI library file location and version
logging.info(f"OpenAI Library Version: {openai.__version__}")
logging.info(f"OpenAI Library File: {openai.__file__}")
