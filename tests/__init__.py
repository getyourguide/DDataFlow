import os

if os.environ.get('ENABLE_OFFLINE_MODE'):
    raise Exception("Offline mode enabled might confuse unit tests, unset it first.")

if os.environ.get('ENABLE_DDATAFLOW'):
    raise Exception("ENABLE_DDATAFLOW env variable might confuse unit tests, unset it first.")
