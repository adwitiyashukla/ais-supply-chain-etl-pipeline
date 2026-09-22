from __future__ import annotations

import argparse
import json
import sys

from waybill import pipeline
from waybill.config import Config


def parse(argv):
    parser = argparse.ArgumentParser(prog="waybill")
    parser.add_argument(
        "stage", choices=sorted(pipeline.STAGES) + ["pipeline"], help="stage to run"
    )
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--days", nargs="*", default=None)
    return parser.parse_args(argv)


def main(argv=None):
    options = parse(argv if argv is not None else sys.argv[1:])
    cfg = Config.load(options.config)
    cfg.paths.ensure()
    if options.stage == "pipeline":
        outcome = pipeline.run_pipeline(cfg, options.days)
    else:
        outcome = pipeline.run_stage(cfg, options.stage, options.days)
    print(json.dumps(outcome, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
