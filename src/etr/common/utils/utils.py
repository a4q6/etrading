import re

def camel_to_snake(name: str) -> str:
    name = re.sub(r'(?<!^)([A-Z]+)(?=[A-Z][a-z]|$)', r'_\1', name)
    # 通常のCamelCaseの変換
    name = re.sub(r'(?<!^)([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()
