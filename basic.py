from dotenv import load_dotenv
from google import genai


load_dotenv()

client = genai.Client()

response = client.models.generate_content(
    model="gemma-4-31b-it",
    contents="Explain how AI works in one sentence.",
)

print(response.text)
