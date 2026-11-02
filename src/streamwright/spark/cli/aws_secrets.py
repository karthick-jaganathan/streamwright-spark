"""AWS Secrets Manager access, imported only by the driver when configured."""

import json
import re

import boto3


def read_secrets(source, job):
    region = ((job.get("runtime") or {}).get("platform") or {}).get("region")
    client = boto3.client("secretsmanager", region_name=region)
    data = {}

    def fetch_value(secret_id):
        reply = client.get_secret_value(SecretId=secret_id)
        raw = reply.get("SecretString")
        if raw is None and "SecretBinary" in reply:
            raw = reply["SecretBinary"].decode("utf-8")
        return raw or ""

    mapping = source.get("mapping")
    if isinstance(mapping, dict):
        for variable, secret_id in mapping.items():
            raw = fetch_value(secret_id)
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    if variable in parsed:
                        data[variable] = parsed[variable]
                    elif len(parsed) == 1:
                        data[variable] = next(iter(parsed.values()))
                    else:
                        data.update(parsed)
                else:
                    data[variable] = raw
            except (ValueError, TypeError):
                data[variable] = raw

    secret_ids = source.get("ids") or source.get("id")
    if isinstance(secret_ids, str):
        secret_ids = [secret_ids]
    elif not isinstance(secret_ids, (list, tuple)):
        secret_ids = []
    for secret_id in secret_ids:
        raw = fetch_value(secret_id)
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                data.update(parsed)
            else:
                leaf = secret_id.split("/")[-1].split(":")[-1]
                leaf = re.sub(r"-[A-Za-z0-9]{6}$", "", leaf)
                data[leaf] = raw
        except (ValueError, TypeError):
            leaf = secret_id.split("/")[-1].split(":")[-1]
            leaf = re.sub(r"-[A-Za-z0-9]{6}$", "", leaf)
            data[leaf] = raw
    return data
