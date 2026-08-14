"""Python steps for the post-load processes (see :mod:`infor_loader.post_process`).

A module here exposes plain callables that a ``python:`` step in a post-process YAML
names as ``<module>.<callable>``. Each takes one argument -- a
:class:`~infor_loader.post_process.StepContext` carrying the live connection, the
destination's identity, the run logger, the step's ``options`` block and the run
date -- and returns the row count it wrote, or None when a row count is meaningless.

Raise on failure. The runner catches it, records the destination FAILED, and puts
the traceback in the run's log file; don't swallow errors to keep a batch "green".
"""
