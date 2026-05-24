"""CLI entry point."""

from __future__ import annotations

import logging

from mobile.logging_config import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    logger.info("mobile CLI ready")


if __name__ == "__main__":
    main()
