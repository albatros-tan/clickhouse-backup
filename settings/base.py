import os

from environs import Env


env = Env()

ROLE = env.str('ROLE', default='local')
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

if ROLE == 'local':
    env.read_env(path=os.path.join(BASE_PATH, '.env'))
