import os
import sys

# Adapta o PYTHONPATH para que a pasta 'src' seja reconhecida durante os testes
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../bronze/src')))
