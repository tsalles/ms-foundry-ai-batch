import logging

# Keep test output clean: assertLogs still captures records, nothing reaches the last-resort handler.
logging.getLogger("foundry_batch").addHandler(logging.NullHandler())
