import sys
import logging

from os import path
logging.basicConfig(level=logging.DEBUG)
sys.path.append(path.join(path.dirname(path.realpath(__file__)), '..'))

if __name__ == "__main__":
    from content_analytics.runner import Runner
    Runner()
