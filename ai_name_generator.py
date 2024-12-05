import os
import openai
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def generate_names(objectives, techniques, num_names=10):
    """
    Generate creative names based on objectives and techniques using OpenAI's GPT-4.

    :param objectives: List of project objectives.
    :param techniques: List of techniques or features.
    :param num_names: Number of name suggestions to generate.
    :return: List of generated names.
    """
    try:
        prompt = (
            f"Generate {num_names} creative and catchy names for a project that focuses on "
            f"{', '.join(objectives)} and utilizes techniques such as {', '.join(techniques)}. "
            f"The names should be unique, memorable, and reflect the innovative nature of the project."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",  # Change to "gpt-3.5-turbo" if GPT-4 is not available
            messages=[
                {"role": "system", "content": "You are a creative assistant that generates project names based on provided objectives and techniques."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100,
            temperature=0.7,
        )

        # Extract and clean names from the response
        raw_text = response.choices[0].message['content'].strip()
        names = [line.strip('- ').strip() for line in raw_text.split('\n') if line.strip()]
        return names[:num_names]

    except Exception as e:
        logging.error(f"Error generating names: {e}")
        return []

if __name__ == "__main__":
    objectives = ["Swarm Intelligence", "Neural Optimization"]
    techniques = ["Adaptive Peer Management", "Ensemble Machine Learning"]

    generated_names = generate_names(objectives, techniques)
    if generated_names:
        logging.info("Generated Project Names:")
        for idx, name in enumerate(generated_names, 1):
            logging.info(f"{idx}. {name}")
    else:
        logging.info("No names generated.")
