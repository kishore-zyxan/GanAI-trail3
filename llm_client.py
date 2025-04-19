import openai

client = openai.OpenAI(
    api_key="d2174fb9-f317-4733-b4d0-85b7631ea051",
    base_url="https://api.sambanova.ai/v1",
)

def analyze_with_llm(text):
    response = client.chat.completions.create(
        model="DeepSeek-R1",
        messages=[
            {"role": "system",
             "content": "You are a JSON formatting machine. Return ONLY valid JSON with no commentary, explanations, or text outside the JSON structure."},
            {"role": "user", "content": f"""
                                        Extract key-value pairs from this document into a FLAT JSON format. Follow these rules STRICTLY:
                                        1. Output MUST be pure JSON only
                                        2. Do NOT nest any keys – all keys must be top-level (flat structure)
                                        3. No markdown formatting
                                        4. No text before/after JSON
                                        5. Ensure proper escaping for quotes
                                        6. If unsure about any data, omit it

            Document:
            {text}
            """}
        ],
        temperature=0.3,
        top_p=0.4
    )
    return response.choices[0].message.content
