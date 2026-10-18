"""
Split exports: one file per distinct combination of `split_by`, named by the date-style templates in `naming`. PROPOSED.

How, with a single Spark action:
  1. copy each split column to a string helper column (so the column stays inside the file)
  2. repartition by the helpers, so every split is in exactly one task, and write with partitionBy(helpers) into a
     staging folder `<path>/_sw_staging/<batch_id>/`  (the one action; the row count comes from observe)
  3. on the driver (file-system calls, no Spark job): list the staging folder, read the split values from the folder
     names, render every final name with `naming.plan_names`, check collisions, max_splits and on_exists for ALL
     splits, and only then rename the files into place and delete the staging folder
A failure before step 3 publishes nothing. A failure during step 3 leaves some files in place; because names come from
the batch id and the pinned batch time, replaying the batch rewrites the same names.
"""

from urllib.parse import unquote

from streamwright.spark.contracts import Observation, WriteReceipt
from streamwright.spark.storage import naming
from streamwright.spark.storage.paths import spark_text


__all__ = ["ExportError", "splits_from_dirs", "write_split_export"]

_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"
STAGING = "_sw_staging"


class ExportError(Exception):
    pass


def splits_from_dirs(relative_files, split_by):
    """
    `relative_files`: paths relative to the staging root such as `__sw_p0=acme/__sw_p1=2026-10-15/part-0000.csv.gz`.
    Returns {split key tuple: (values dict, [file, ...])}. A null split value is an error.
    """
    helper = ["__sw_p%d" % i for i in range(len(split_by))]
    found = {}
    for rel in relative_files:
        parts = rel.split("/")
        folders, filename = parts[:-1], parts[-1]
        if len(folders) != len(helper):
            raise ExportError("unexpected staging layout %r" % rel)
        values = []
        for expected, folder in zip(helper, folders):
            name, _, raw = folder.partition("=")
            if name != expected:
                raise ExportError("unexpected staging folder %r" % folder)
            if raw == _NULL_PARTITION:
                raise ExportError("a split column is null in some rows (%s); filter or coalesce it in the output's sql"
                                  % split_by[helper.index(expected)])
            values.append(unquote(raw))
        key = tuple(values)
        entry = found.setdefault(key, (dict(zip(split_by, values)), []))
        entry[1].append(rel)
    return found


def write_split_export(spark, frame, ctx, output, arguments, root_uri, file_template, layout_template):
    """`root_uri`: the checked destination folder in Hadoop form (adapter.to_hadoop). Returns a WriteReceipt. Needs Spark; not run in unit tests."""
    from pyspark.sql import Observation as SparkObservation
    from pyspark.sql import functions as F

    split_by = list(arguments.get("split_by") or [])
    fmt, options = arguments["format"], dict(arguments.get("options") or {})
    staging = root_uri.rstrip("/") + "/" + STAGING + "/" + ctx.batch_id
    helpers = ["__sw_p%d" % i for i in range(len(split_by))]
    for index, column in enumerate(split_by):
        frame = frame.withColumn(helpers[index], F.col(column).cast("string"))
    metrics = SparkObservation("export_" + output)
    frame = frame.observe(metrics, F.count(F.lit(1)).alias("rows"))
    frame = frame.repartition(*[F.col(h) for h in helpers]) if helpers else frame.coalesce(1)
    writer = frame.write.format(fmt).mode("overwrite").option("maxRecordsPerFile", 0)
    for key, value in options.items():
        writer = writer.option(str(key), spark_text(value))
    if helpers:
        writer = writer.partitionBy(*helpers)
    fs, path_cls = _filesystem(spark, staging)
    try:
        writer.save(staging)                                      # the single Spark action
        rows = int(metrics.get.get("rows", 0))
        listed = _list_files(fs, path_cls, staging)
        if not helpers:
            splits = {(): ({}, listed)} if listed else {}
        else:
            splits = splits_from_dirs(listed, split_by)
        limit = int(arguments.get("max_splits", 500))
        if len(splits) > limit:
            raise ExportError("%d splits exceed max_splits (%d); nothing was published" % (len(splits), limit))
        for key, (_, files) in splits.items():
            if len(files) != 1:
                raise ExportError("split %r produced %d files; this format cannot be written as one file per split"
                                  % (key, len(files)))
        names = naming.plan_names(file_template, layout_template,
                                  [(key, values) for key, (values, _) in sorted(splits.items())],
                                  ctx.batch_ts, ctx.batch_id)
        published = _publish(fs, path_cls, staging, root_uri, splits, names, arguments.get("on_exists", "error"))
        return WriteReceipt(output=output, target=root_uri, rows=rows, files=sorted(published),
                            observations=[Observation(output, {"rows": rows, "splits": len(splits)})])
    finally:
        try:
            fs.delete(path_cls(staging), True)
        except Exception:                                         # cleanup must not hide the real error
            pass


def _filesystem(spark, uri):
    path_cls = spark._jvm.org.apache.hadoop.fs.Path
    return path_cls(uri).getFileSystem(spark._jsc.hadoopConfiguration()), path_cls


def _list_files(fs, path_cls, staging):
    base = path_cls(staging)
    if not fs.exists(base):
        return []
    prefix = fs.makeQualified(base).toString().rstrip("/") + "/"
    out, it = [], fs.listFiles(base, True)
    while it.hasNext():
        item = it.next().getPath()
        name = item.getName()
        if name.startswith(("_", ".")) or name.endswith(".crc"):
            continue
        out.append(item.toString()[len(prefix):])
    return sorted(out)


def _publish(fs, path_cls, staging, root_uri, splits, names, on_exists):
    jobs = []
    for key, (_, files) in splits.items():
        source = path_cls(staging + "/" + files[0])
        target = path_cls(root_uri.rstrip("/") + "/" + names[key])
        jobs.append((source, target))
        if fs.exists(target) and on_exists == "error":
            raise ExportError("%s already exists (on_exists: error); nothing was published" % names[key])
    published = []
    for source, target in jobs:
        if fs.exists(target):
            if on_exists == "skip":
                continue
            fs.delete(target, False)
        fs.mkdirs(target.getParent())
        if not fs.rename(source, target):
            raise ExportError("could not move a staged file into place: %s" % target.getName())
        published.append(target.toString())
    return published
