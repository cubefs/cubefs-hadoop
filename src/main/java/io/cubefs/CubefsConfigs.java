// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package io.cubefs;

import org.apache.hadoop.fs.CommonConfigurationKeys;

public class CubefsConfigs extends CommonConfigurationKeys {

    public static final String CFS_VOLUME_NAME_KEY = "cfs.volume.name";
    public static final String CFS_VOLUME_NAME_DEFAULT = null;

    public static final String CFS_MASTER_ADDRESS_KEY = "cfs.master.address";
    public static final String CFS_MASTER_ADDRESS_KEY_DEFAULT = null;

    public static final String CFS_LOG_DIR_KEY = "cfs.log.dir";
    public static final String CFS_LOG_DIR_KEY_DEFAULT = "/tmp/cfs-access.log";

    public static final String CFS_LOG_LEVEL_KEY = "cfs.log.level";
    public static final String CFS_LOG_LEVEL_KEY_DEFAULT = "INFO";

    public static final String CFS_SUPER_USER_NAME = "cfs.super.user.name";
    public static final String CFS_SUPER_USER_NAME_DEFAULT = "hdfs";

    public static final String CFS_SUPER_USER_GROUP = "dfs.permissions.superusergroup";
    public static final String CFS_SUPER_USER_GROUP_DEFAULT = "supergroup";

    public static final String CFS_MIN_BUFFER_SIZE = "cfs.min.buffersize";
    public static final int CFS_MIN_BUFFER_SIZE_DEFAULT = 1 << 23;

    public static final String CFS_BCACHE_ENABLE = "cfs.enable.bcache";
    public static final String CFS_BCACHE_ENABLE_DEFAULT = "false";

    public static final String CFS_ACCESS_KEY = "cfs.access.key";
    public static final String CFS_SECRET_KEY = "cfs.secret.key";

    public static final String CFS_MIN_READ_BUFFER_SIZE = "cfs.min.read.buffersize";
    public static final int CFS_MIN_READ_BUFFER_SIZE_DEFAULT = 128 << 10;
}
