"""Turbine pipeline: ingest → clean → stats → anomalies → store."""

from importlib.metadata import version

from turbine_pipeline.pipeline import PipelineResult, run_pipeline, run_pipeline_range

__all__ = ["run_pipeline", "run_pipeline_range", "PipelineResult"]
__version__ = version("turbine-pipeline")
