--
-- Current Database: `sync_config`
--

USE `sync_config`;

--
-- Dumping data for table `database`
--

/*!40000 ALTER TABLE `database`
    ENABLE KEYS */;
INSERT INTO `database` (`id`, `sync_type`, `db_show_name`, `db_type`, `db_host`, `db_port`, `db_name`, `db_args`, `db_user`, `db_password`, `is_active`, `department_id`, `created_at`, `updated_at`)
VALUES (1, 'db2jdy', 'quality_market发送', 'MySQL', '10.63.136.4', 3306, 'Quality_Market', '', 'API_user', 'API_Quality2025!', 1, 2, '2025-11-06 13:40:14', '2025-11-06 13:47:25')
     , (6, 'jdy2db', 'quality_market接收', 'MySQL', '10.63.136.4', 3306, 'Quality_Market', '', 'API_user', 'API_Quality2025!', 1, 2, '2025-11-06 13:47:07', '2025-11-06 13:47:17')
     , (7, 'db2jdy', 'sl_cicd', 'MySQL', '10.63.136.44', 3306, 'sl_cicd', '', '6000357', '6000357', 1, 4, '2025-11-11 17:03:05', '2025-11-11 18:22:26');
/*!40000 ALTER TABLE `database`
    ENABLE KEYS */;


--
-- Dumping data for table `department`
--

/*!40000 ALTER TABLE `department`
    DISABLE KEYS */;
INSERT INTO `department` (`id`, `department_name`, `department_full_name`, `is_active`, `created_at`, `updated_at`)
VALUES (2, 'quality_dept', '质量部门', 1, '2025-11-06 13:27:01', '2025-11-11 16:18:02')
     , (3, 'eco_dept', '生态运营业务部门', 1, '2025-11-07 16:44:16', '2025-11-07 16:44:16')
     , (4, 'eco_develop_dept', '电控开发业务单元', 1, '2025-11-11 16:17:22', '2025-11-11 16:18:51')
/*!40000 ALTER TABLE `department`
    ENABLE KEYS */;

--
-- Dumping data for table `jdy_key_info`
--

/*!40000 ALTER TABLE `jdy_key_info` DISABLE KEYS */;
INSERT INTO `jdy_key_info` (`id`, `department_id`, `api_key`, `description`, `created_at`, `updated_at`) VALUES (1,2,'RD0p5m5vtktvgldnYPbXgG1BT06ruZ9l','质量部门','2025-11-06 13:27:28','2025-11-06 15:25:49');
/*!40000 ALTER TABLE `jdy_key_info` ENABLE KEYS */;

--
-- Dumping data for table `sync_tasks`
--

INSERT INTO `sync_tasks` ( `id`, `sync_type`, `task_name`, `database_id`, `table_name`, `business_keys`, `app_id`, `entry_id`, `department_id`, `sync_mode`, `incremental_fields`, `incremental_interval`, `full_sync_time`, `source_filter_sql`, `last_binlog_file`, `last_binlog_pos`, `sync_status`
                         , `last_sync_time`, `is_full_sync_first`, `is_delete_first`, `is_active`, `send_error_log_to_wecom`, `wecom_robot_webhook_url`, `created_at`, `updated_at`, `api_secret`, `daily_sync_time`, `daily_sync_type`, `json_as_string`, `label_to_pinyin`, `webhook_url`)
VALUES ( 2, 'db2jdy', '云图维修工单_view', 1, '云图维修工单_view', '["工单号","备件名称"]', '6607ac2c0d1f1a7ae4807f44', '6901d00ba06798a9b9ce1397', 2, 'FULL_SYNC', NULL, 120, '08:45:00', NULL, NULL, NULL, 'idle', '2025-11-12 08:45:34', 0, 0, 1, 0, NULL, '2025-11-03 10:28:40', '2025-11-12 09:45:03', NULL
       , NULL, NULL, 0, 0, NULL)
     , ( 3, 'db2jdy', 'DDM维修工单_view', 1, 'DDM维修工单_view', '["三包单号"]', '6607ac2c0d1f1a7ae4807f44', '6901d0f6d3a8b2532dc22629', 2, 'FULL_SYNC', '["审核时间"]', 240, '08:30:00', NULL, NULL, NULL, 'idle', '2025-11-12 08:33:26', 0, 0, 1, 0, NULL, '2025-11-03 10:29:28', '2025-11-12 08:33:26', NULL
       , NULL, NULL, 0, 0, NULL)
     , ( 4, 'db2jdy', 'R1000记录表_车型', 1, 'R1000记录表_车型', '["RK_ID"]', '6607ac2c0d1f1a7ae4807f44', '68da5a38835d103c4be2a9c0', 2, 'INCREMENTAL', '["Input_Time"]', 240, NULL, NULL, NULL, NULL, 'idle', '2025-11-12 11:36:35', 0, 0, 1, 0, NULL, '2025-11-03 10:29:59', '2025-11-12 11:36:36', NULL, NULL
       , NULL, 0, 0, NULL)
     , ( 5, 'db2jdy', '车型CCC分工明细', 1, '车型CCC分工明细', '["分工ID"]', '6607ac2c0d1f1a7ae4807f44', '68d8ff17644aa36018327fa5', 2, 'INCREMENTAL', '["Updated_time"]', 120, NULL, NULL, NULL, NULL, 'idle', '2025-11-12 11:36:35', 0, 0, 1, 0, NULL, '2025-11-03 10:30:37', '2025-11-12 11:36:36', NULL, NULL
       , NULL, 0, 0, NULL)
     , ( 6, 'db2jdy', 'dwd_quality_order_ai_analysis_info', 1, 'dwd_quality_order_ai_analysis_info', '["business_id"]', '6607ac2c0d1f1a7ae4807f44', '68d623a8ab68110e273da0aa', 2, 'FULL_SYNC', '["created_time"]', 120, '04:00:22', 'date(created_time) >= current_date() - interval 1 day', NULL, NULL, 'idle'
       , '2025-11-10 19:32:48', 0, 0, 0, 0, NULL, '2025-11-03 10:31:10', '2025-11-11 09:20:53', NULL, NULL, NULL, 0, 0, NULL)
     , ( 7, 'db2jdy', 'dwd_work_order_union_df', 1, 'dwd_work_order_union_df', '["business_key"]', '6607ac2c0d1f1a7ae4807f44', '68da49db23b0d617477d15f2', 2, 'BINLOG', '["created_time"]', 240, '02:30:22', NULL, 'mysql-bin.000045', 865280525, 'running', '2025-11-12 10:39:16', 0, 0, 1, 0, NULL
       , '2025-11-03 10:31:39', '2025-11-12 11:36:33', NULL, NULL, NULL, 0, 0, NULL)
     , ( 8, 'db2jdy', '问题管理主表', 1, '问题管理主表', '["问题ID"]', '6607ac2c0d1f1a7ae4807f44', '68da51b2d0c0defe130a4bec', 2, 'INCREMENTAL', '["Updated_time"]', 120, '11:10:00', NULL, NULL, NULL, 'idle', '2025-11-12 11:36:36', 0, 0, 1, 0, NULL, '2025-11-03 10:32:11', '2025-11-12 11:36:38', NULL, NULL
       , NULL, 0, 0, NULL)
     , ( 9, 'db2jdy', 'R1000记录表_车型_CCC', 1, 'R1000记录表_车型_CCC', '["RK_ID"]', '6607ac2c0d1f1a7ae4807f44', '68d9dffb644aa360184fdad1', 2, 'INCREMENTAL', '["Input_Time"]', 120, '13:27:00', 'RK_ID > 441308', NULL, NULL, 'idle', '2025-11-12 11:36:36', 0, 0, 1, 0, NULL, '2025-11-03 10:32:38'
       , '2025-11-12 11:36:36', NULL, NULL, NULL, 0, 0, NULL)
     , ( 10, 'db2jdy', 'TGW记录表_车型_CCC', 1, 'TGW记录表_车型_CCC', '["TGW_ID"]', '6607ac2c0d1f1a7ae4807f44', '68d9e22e9b1b364c49b2b829', 2, 'INCREMENTAL', '["Input_Time"]', 120, '09:44:22', NULL, NULL, NULL, 'idle', '2025-11-12 11:36:35', 0, 0, 1, 0, NULL, '2025-11-03 10:33:05', '2025-11-12 11:36:36'
       , NULL, NULL, NULL, 0, 0, NULL)
     , ( 11, 'db2jdy', '指标汇总表', 1, '指标汇总表', '["序号"]', '6607ac2c0d1f1a7ae4807f44', '68f0b70936c4efcf018c08c9', 2, 'BINLOG', '[]', 5, '15:09:00', NULL, 'mysql-bin.000045', 510216225, 'running', '2025-11-12 10:39:15', 0, 0, 1, 0, NULL, '2025-11-03 10:37:10', '2025-11-12 11:36:31', NULL, NULL
       , NULL, 0, 0, NULL)
     , ( 12, 'db2jdy', 'CCC_备件名称_view', 1, 'CCC_备件名称_view', '["CCC,备件名称"]', '6607ac2c0d1f1a7ae4807f44', '69033b824e68b3ce339dc2ce', 2, 'FULL_SYNC', '[]', 5, '06:00:00', NULL, NULL, NULL, 'idle', '2025-11-10 19:05:03', 1, 0, 1, 0, NULL, '2025-11-03 10:40:27', '2025-11-11 09:21:30', NULL, NULL
       , NULL, 0, 0, NULL)
     , ( 26, 'db2jdy', 'dwd_quality_order_ai_analysis_info_binlog', 1, 'dwd_quality_order_ai_analysis_info', '["business_id"]', '6607ac2c0d1f1a7ae4807f44', '68d623a8ab68110e273da0aa', 2, 'BINLOG', '[]', 5, '02:30:22', NULL, 'mysql-bin.000045', 563096068, 'running', '2025-11-12 10:39:14', 0, 0, 1, 0, NULL
       , '2025-11-10 11:31:03', '2025-11-12 11:36:30', NULL, NULL, 'DAILY', 0, 0, NULL);


--
-- Dumping data for table `users`
--

INSERT INTO `users` (`id`, `username`, `password`, `department_id`, `is_superuser`, `is_active`, `created_at`, `updated_at`)
VALUES (2, '202105352', '$pbkdf2-sha256$29000$612r9Z5TCqH0HsO4F6IUIg$t56lTWd3QdrhlmHacSYi8vV2lAZOeDZLCESVlzUToj8', 2, 0, 1, '2025-11-06 13:27:12', '2025-11-06 15:15:32')
     , (3, '202313850', '$pbkdf2-sha256$29000$9x4j5BxjTKlVyjmHMCbkvA$n0t8jrgUBuReJWuAVx5yu8uMe9bMvcX/RZ0LoLfXa1w', 3, 0, 1, '2025-11-07 16:44:30', '2025-11-07 16:52:31')
     , (4, '6000357', '$pbkdf2-sha256$29000$Wuv933tPqRUihLD2fu8dow$KSv0jCGgQx/3iP8BXtihGPKQGNVJNit/d9cILXt54WU', 4, 0, 1, '2025-11-11 16:19:12', '2025-11-11 16:19:12');
