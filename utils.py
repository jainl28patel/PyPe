import re

def get_parameters_from_action(input_str: str):
    matches = re.findall(r"\{(.*?)\}", input_str)
    return matches


