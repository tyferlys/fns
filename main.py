from loguru import logger

from parsers.Source26.source26 import Source26
from parsers.Source27.source27 import Source27


def main():
    logger.info("Старт парсинга и занесения данных и источника 26")
    source26 = Source26()
    source26.start_parse()

    logger.info("Старт парсинга и занесения данных и источника 27")
    source27 = Source27()
    source27.start_parse()


if __name__ == "__main__":
    main()