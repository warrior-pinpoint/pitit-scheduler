BOOLEAN_STATES = {
    '1': True,
    'yes': True,
    'true': True,
    'True': True,
    'on': True,
    '0': False,
    'no': False,
    'false': False,
    'False': False,
    'off': False,
}


def str_to_bool(value):
    if value is None or value == '':
        return None

    if isinstance(value, str) and value in BOOLEAN_STATES:
        return BOOLEAN_STATES[value]
    return bool(value)