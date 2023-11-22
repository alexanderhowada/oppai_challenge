import os
import json
import pandas as pd
from openai import OpenAI


class OpenAITranslate:
    def __init__(self, *args, **kwargs):
        self.cli = OpenAI(*args, **kwargs)
        self.model = "gpt-3.5-turbo-1106"        
        
    def _build_context(self):
        base_message = """Translate the messages to english, the source language.""" \
            """An unknown language must be left with the "" value.""" \
            """Return a JSON in the format: {"translations": ["translation 1", "translation 2"], "source_language": ["en", "fr"]}"""  # \
        return [{"role": "user", "content": base_message}]
    
    def __call__(self, messages: list[str]):
        context = self._build_context()
        context[0]['content'] += json.dumps(messages)
        completion = self.cli.chat.completions.create(
            model=self.model,
            response_format={"type": "json_object"},
            messages=context
        )
        r = json.loads(str(completion.choices[0].message.content))
        return r
    
    def translate(*args, **kwargs):
        return self.__call__(*args, **kwargs)
    
    def get_df(self, *args, **kwargs):
        d = self.__call__(*args, **kwargs)
        return pd.DataFrame(d)
    
class OpenAISentimentAnalysis(OpenAITranslate):
    def _build_context(self):
        base_message = """Infer the sentiment of the phrases as positive (p), neutral (0), negative (n) or unknown (u)""" \
            """Return a JSON in the format: {"sentiment": ["p", "0"]}"""  # \
        return [{"role": "user", "content": base_message}]

class OpenAIPostReport(OpenAITranslate):
    def _build_context(self):
        base_message = """Summarize these messages.""" \
            """For context, these messages were written in a adult game Patreon.""" \
            """Return a single JSON in the format: {"summary": ["response here"]}."""
        return [{"role": "user", "content": base_message}]
    
def open_ai_request(self):