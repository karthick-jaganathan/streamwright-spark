#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""platform: local. The driver runs in this process (Spark local[*]); nothing is submitted."""

import os

__all__ = ["LocalLauncher"]


class LocalLauncher:
    platform = "local"
    contract_version = "1"

    def check_platform(self, platform):
        return []

    def check_lock(self, lock, platform=None):
        return []

    def preview(self, job_path, argv, conf, lock, *, platform, lock_path=None, files=()):
        command = ["spark-submit", "--master", "local[*]"]
        for key, value in sorted(conf.items()):
            command += ["--conf", "%s=%s" % (key, value)]
        return {"command": [*command, os.path.basename(job_path), *list(argv)]}
