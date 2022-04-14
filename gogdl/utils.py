def strip_quotes(text: str) -> str:
    if text[0] == '"' and text[-1] == '"':
        return text[1:-1]
