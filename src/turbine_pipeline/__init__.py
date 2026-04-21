"""Turbine pipeline: ingest → clean → stats → anomalies → store."""

from importlib.metadata import version

from turbine_pipeline.pipeline import PipelineResult, run_pipeline

__all__ = ["run_pipeline", "PipelineResult"]
__version__ = version("turbine-pipeline")