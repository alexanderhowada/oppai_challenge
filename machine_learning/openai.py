import os
import json
import pandas as pd
# from openai import OpenAI
import requests
from copy import deepcopy

# OpenAI is not working on Databricks
# class OpenAITranslate:
#     def __init__(self, *args, **kwargs):
#         self.cli = OpenAI(*args, **kwargs)
#         self.model = "gpt-3.5-turbo-1106"        
        
#     def _build_context(self):
#         base_message = """Translate the messages to english, the source language.""" \
#             """An unknown language must be left with the "" value.""" \
#             """Return a JSON in the format: {"translations": ["translation 1", "translation 2"], "source_language": ["en", "fr"]}"""  # \
#         return [{"role": "user", "content": base_message}]
    
#     def __call__(self, messages: list[str]):
#         context = self._build_context()
#         context[0]['content'] += json.dumps(messages)
#         completion = self.cli.chat.completions.create(
#             model=self.model,
#             response_format={"type": "json_object"},
#             messages=context
#         )
#         r = json.loads(str(completion.choices[0].message.content))
#         return r
    
#     def translate(*args, **kwargs):
#         return self.__call__(*args, **kwargs)
    
#     def get_df(self, *args, **kwargs):
#         d = self.__call__(*args, **kwargs)
#         return pd.DataFrame(d)
    
# class OpenAISentimentAnalysis(OpenAITranslate):
#     def _build_context(self):
#         base_message = """Infer the sentiment of the phrases as positive (p), neutral (0), negative (n) or unknown (u)""" \
#             """Return a JSON in the format: {"sentiment": ["p", "0"]}"""  # \
#         return [{"role": "user", "content": base_message}]

# class OpenAIPostReport(OpenAITranslate):
#     def _build_context(self):
#         base_message = """Summarize these messages.""" \
#             """For context, these messages were written in a adult game Patreon.""" \
#             """Return a single JSON in the format: {"summary": ["response here"]}."""
#         return [{"role": "user", "content": base_message}]
    

class OpenAIRequests:
    """Make direct request to OpenAI without using the Python api (bugged in Databricks)."""

    def __init__(self, openai_api_key):
        self.api_key = openai_api_key
        self.cost_per_100k_token = {
            'input': 0.001, 'output': 0.002
        }

    def get_headers(self):
        return  {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

    def _build_context(self, message):
        return [{"role": "user", "content": message}]

    def chat_completion(self, message):
        url = "https://api.openai.com/v1/chat/completions"
        data = {
            "model": "gpt-3.5-turbo-1106",
            "response_format": {"type": "json_object"},
            "messages": self._build_context(message),
        }
        headers = self.get_headers()

        self.response = requests.post(url, headers=headers, json=data)

        return deepcopy(self.response)
    
    def calculate_cost(self, r=None):
        r = r or self.response
        cost = r.json()['usage']['prompt_tokens']*self.cost_per_100k_token['input'] \
            + r.json()['usage']['completion_tokens']*self.cost_per_100k_token['output']
        cost /= 1000
        return cost
    
    def get_content_df(self, r=None):
        r = r or self.response

        content = r.json()['choices']
        new_content = []
        for i, c in enumerate(content):
            new_content.append(json.loads(content[i]['message']['content']))
        
        return pd.DataFrame(new_content)












