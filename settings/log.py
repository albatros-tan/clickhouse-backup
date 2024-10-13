import logging


logging.basicConfig(level=logging.INFO, filename="log.log", format="%(asctime)s [%(levelname)s] %(message)s")

app_log = logging.getLogger(__name__)
app_log.setLevel(logging.INFO)
