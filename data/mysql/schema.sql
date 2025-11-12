--
-- Current Database: `sync_config`
--

CREATE
DATABASE IF NOT EXISTS `sync_config`;

USE
`sync_config`;

--
-- Table structure for table `database`
--

-- DROP TABLE IF EXISTS `database`;
CREATE TABLE `database`
(
    `id`            int          NOT NULL AUTO_INCREMENT,
    `sync_type`     varchar(50)  NOT NULL COMMENT '同步类型: db2jdy (数据库->简道云), jdy2db (简道云->数据库)',
    `db_show_name`  varchar(50)  NOT NULL COMMENT '数据库显示名称 (e.g., 质量部门专用数据库)',
    `db_type`       varchar(50)  NOT NULL COMMENT '数据库类型(e.g. MySQL, PostgreSQL, Oracle, SQL Server)',
    `db_host`       varchar(255) NOT NULL COMMENT '数据库主机',
    `db_port`       int          NOT NULL COMMENT '数据库端口',
    `db_name`       varchar(50)  NOT NULL COMMENT '数据库名称',
    `db_args`       varchar(255) DEFAULT NULL COMMENT '数据库连接参数(e.g. charset=utf8mb4)',
    `db_user`       varchar(50)  NOT NULL COMMENT '数据库用户名',
    `db_password`   varchar(255) NOT NULL COMMENT '数据库密码',
    `is_active`     tinyint(1) DEFAULT NULL COMMENT '是否激活',
    `department_id` int          NOT NULL COMMENT '关联的租户ID',
    `created_at`    datetime     DEFAULT NULL COMMENT '创建时间',
    `updated_at`    datetime     DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_dept_db_show_name` (`department_id`,`db_show_name`),
    UNIQUE KEY `uq_dept_db_connection_info` (`department_id`,`db_show_name`,`db_type`,`db_host`,`db_port`,`db_name`,`db_user`),
    CONSTRAINT `database_ibfk_1` FOREIGN KEY (`department_id`) REFERENCES `department` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `department`
--

-- DROP TABLE IF EXISTS `department`;
CREATE TABLE `department`
(
    `id`                   int          NOT NULL AUTO_INCREMENT COMMENT '关联的租户部门外部ID',
    `department_name`      varchar(100) NOT NULL COMMENT '关联的租户部门全称, e.g., ''软件部门''',
    `department_full_name` varchar(255) DEFAULT NULL COMMENT '关联的租户部门简称(英文), e.g., ''sft_dept''',
    `is_active`            tinyint(1) DEFAULT NULL COMMENT '是否激活',
    `created_at`           datetime     DEFAULT NULL COMMENT '创建时间',
    `updated_at`           datetime     DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `department_name` (`department_name`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `form_fields_mapping`
--

-- DROP TABLE IF EXISTS `form_fields_mapping`;
CREATE TABLE `form_fields_mapping`
(
    `id`               int          NOT NULL AUTO_INCREMENT,
    `task_id`          int          NOT NULL COMMENT '关联的任务ID',
    `form_name`        varchar(255) DEFAULT NULL COMMENT '简道云表单名',
    `widget_name`      varchar(255) NOT NULL COMMENT '字段ID (e.g., _widget_xxx, 用于 API 提交)',
    `widget_alias`     varchar(255) NOT NULL COMMENT '字段后端别名 (name, 用于 API 查询/匹配)',
    `label`            varchar(255) NOT NULL COMMENT '字段前端别名 (label)',
    `type`             varchar(255) NOT NULL COMMENT '字段类型 (type)',
    `data_modify_time` datetime     DEFAULT NULL COMMENT '表单数据修改时间',
    `last_updated`     datetime     DEFAULT NULL,
    `created_at`       datetime     DEFAULT NULL COMMENT '创建时间',
    `updated_at`       datetime     DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_task_widget_alias` (`task_id`,`widget_alias`),
    UNIQUE KEY `uq_task_widget_name` (`task_id`,`widget_name`),
    KEY                `idx_task_id` (`task_id`),
    CONSTRAINT `form_fields_mapping_ibfk_1` FOREIGN KEY (`task_id`) REFERENCES `sync_tasks` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=248491 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `jdy_key_info`
--

-- DROP TABLE IF EXISTS `jdy_key_info`;
CREATE TABLE `jdy_key_info`
(
    `id`            int          NOT NULL AUTO_INCREMENT,
    `department_id` int          NOT NULL COMMENT '关联的租户ID',
    `api_key`       varchar(255) NOT NULL COMMENT '该项目专属的 API Key',
    `description`   varchar(255) DEFAULT NULL COMMENT '该项目描述',
    `created_at`    datetime     DEFAULT NULL COMMENT '创建时间',
    `updated_at`    datetime     DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `department_id` (`department_id`),
    CONSTRAINT `jdy_key_info_ibfk_1` FOREIGN KEY (`department_id`) REFERENCES `department` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `sync_err_log`
--

-- DROP TABLE IF EXISTS `sync_err_log`;
CREATE TABLE `sync_err_log`
(
    `id`              int          NOT NULL AUTO_INCREMENT,
    `sync_type`       varchar(10)  NOT NULL DEFAULT 'db2jdy' COMMENT '任务同步类型: db2jdy, jdy2db',
    `task_id`         int                   DEFAULT NULL COMMENT '关联的任务ID',
    `app_id`          varchar(100)          DEFAULT NULL,
    `entry_id`        varchar(100)          DEFAULT NULL,
    `table_name`      varchar(255)          DEFAULT NULL,
    `department_id`   int          NOT NULL COMMENT '关联的租户ID',
    `department_name` varchar(255) NOT NULL COMMENT '关联的租户部门英文简称, e.g., ''dpt_a''',
    `error_message`   text         NOT NULL,
    `traceback`       text COLLATE utf8mb4_general_ci,
    `payload`         longtext COLLATE utf8mb4_general_ci,
    `timestamp`       datetime              DEFAULT NULL COMMENT '发生错误时间',
    `created_at`      datetime              DEFAULT NULL COMMENT '创建时间',
    `updated_at`      datetime              DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY               `department_id` (`department_id`),
    KEY               `idx_task_time` (`task_id`,`timestamp`),
    CONSTRAINT `sync_err_log_ibfk_1` FOREIGN KEY (`department_id`) REFERENCES `department` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=19056 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `sync_tasks`
--

-- DROP TABLE IF EXISTS `sync_tasks`;
CREATE TABLE `sync_tasks`
(
    `id`                      int         NOT NULL AUTO_INCREMENT COMMENT '任务ID',
    `sync_type`               varchar(10) NOT NULL DEFAULT 'db2jdy' COMMENT '任务同步类型: db2jdy, jdy2db',
    `task_name`               varchar(255)         DEFAULT NULL COMMENT '任务名称',
    `database_id`             int         NOT NULL COMMENT '数据库ID',
    `table_name`              varchar(255)         DEFAULT NULL COMMENT '数据库表名 (源或目标)',
    `business_keys`           JSON                 DEFAULT NULL COMMENT '数据表主键字段名 (复合主键用英文逗号分隔)',
    `app_id`                  varchar(100)         DEFAULT NULL COMMENT '简道云应用ID (jdy2db 模式下可由 webhook 自动填充)',
    `entry_id`                varchar(100)         DEFAULT NULL COMMENT '简道云表单ID (jdy2db 模式下可由 webhook 自动填充)',
    `department_id`           int         NOT NULL COMMENT '关联的租户ID',
    `sync_mode`               varchar(50)          DEFAULT NULL COMMENT '同步模式 (FULL_REPLACE, INCREMENTAL, BINLOG)',
    `incremental_fields`      JSON                 DEFAULT NULL COMMENT '增量同步依赖的时间字段 (仅 INCREMENTAL 模式)',
    `incremental_interval`    int                  DEFAULT NULL COMMENT '增量同步间隔 (分钟) (仅 INCREMENTAL 模式)',
    `full_sync_time`          time                 DEFAULT NULL COMMENT '全量同步时间 (仅 FULL_REPLACE 模式)',
    `source_filter_sql`       text COMMENT '源数据库过滤 SQL (用于 INCREMENTAL 和 FULL_REPLACE模式)',
    `last_binlog_file`        varchar(255)         DEFAULT NULL COMMENT '上次同步的 binlog 文件 (用于 BINLOG)',
    `last_binlog_pos`         int                  DEFAULT NULL COMMENT '上次同步的 binlog 位置 (用于 BINLOG)',
    `sync_status`             varchar(20)          DEFAULT NULL COMMENT '任务状态 (idle, running, error, disabled)',
    `last_sync_time`          datetime             DEFAULT NULL COMMENT '上次同步时间 (用于 INCREMENTAL 和 FULL_REPLACE模式)',
    `is_full_sync_first`      tinyint(1) DEFAULT NULL COMMENT '是否首次同步执行全量覆盖同步 (用于 INCREMENTAL, BINLOG, jdy2db)',
    `is_delete_first`         tinyint(1) DEFAULT NULL COMMENT '全量同步是否清空目标数据',
    `is_active`               tinyint(1) DEFAULT NULL COMMENT '任务是否启用',
    `send_error_log_to_wecom` tinyint(1) NOT NULL COMMENT '是否发送错误日志到企微',
    `wecom_robot_webhook_url` varchar(500)         DEFAULT NULL COMMENT '企业微信机器人 Webhook URL',
    `created_at`              datetime             DEFAULT NULL COMMENT '创建时间',
    `updated_at`              datetime             DEFAULT NULL COMMENT '更新时间',
    `api_secret`              varchar(255)         DEFAULT NULL COMMENT '简道云 Webhook API Secret (jdy2db 专属)',
    `daily_sync_time`         time                 DEFAULT NULL COMMENT '简道云同步数据库的每日全量同步时间',
    `daily_sync_type`         varchar(20)          DEFAULT NULL COMMENT 'DAILY, ONCE',
    `json_as_string`          tinyint(1) NOT NULL COMMENT '是否将JSON存储为字符串',
    `label_to_pinyin`         tinyint(1) NOT NULL COMMENT '是否将label转换为拼音作为列名',
    `webhook_url`             varchar(500)         DEFAULT NULL COMMENT '简道云同步数据库的WebHook URL (后端自动生成)',
    PRIMARY KEY (`id`),
    KEY                       `database_id` (`database_id`),
    KEY                       `department_id` (`department_id`),
    KEY                       `idx_mode_status` (`sync_mode`,`sync_status`),
    CONSTRAINT `sync_tasks_ibfk_1` FOREIGN KEY (`database_id`) REFERENCES `database` (`id`),
    CONSTRAINT `sync_tasks_ibfk_2` FOREIGN KEY (`department_id`) REFERENCES `department` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Table structure for table `users`
--

-- DROP TABLE IF EXISTS `users`;
CREATE TABLE `users`
(
    `id`            int          NOT NULL AUTO_INCREMENT,
    `username`      varchar(120) NOT NULL COMMENT '用户名',
    `password`      varchar(128) DEFAULT NULL COMMENT '密码',
    `department_id` int          NOT NULL COMMENT '关联的租户ID',
    `is_superuser`  tinyint(1) DEFAULT NULL COMMENT '是否是超级用户',
    `is_active`     tinyint(1) DEFAULT NULL COMMENT '是否激活',
    `created_at`    datetime     DEFAULT NULL COMMENT '创建时间',
    `updated_at`    datetime     DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `username` (`username`),
    KEY             `department_id` (`department_id`),
    CONSTRAINT `users_ibfk_1` FOREIGN KEY (`department_id`) REFERENCES `department` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
