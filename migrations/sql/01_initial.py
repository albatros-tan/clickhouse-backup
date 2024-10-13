#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tables for logging the operation of the service

"""


from yoyo import step



steps = [
    step(
        '''create schema if not exists clickhouse_backup;''',
        '''drop schema if exists clickhouse_backup;'''
    ),
    step(
        '''
        create table if not exists clickhouse_backup.backup_history
        (
        	id				serial					primary key,
        	backup_guid		uuid					not null,
        	table_name		varchar(60)				not null,
        	partition_key	varchar(36)				not null,
        	count			int						not null,
        	created			timestamp default timezone('utc'::text, now()),
        	file_name		varchar(255)			not null,
        	execution_time	int						not null
        );
        ''',
        '''drop table if exists clickhouse_backup.backup_history;'''
    ),
    step(
        '''
        create index if not exists backup_guid_index on clickhouse_backup.backup_history (backup_guid);
        create index if not exists backup_guid_table_index on clickhouse_backup.backup_history (backup_guid, table_name);
        ''',
        '''
        drop index if exists backup_guid_index;
        drop index if exists backup_guid_table_index;
        '''
    ),
    step(
        '''
        create table if not exists clickhouse_backup.backup_log
        (
        	backup_guid		uuid					not null,
        	table_name		varchar(60)				not null,
        	partition_key	varchar(36)				not null,
        	event			text					not null,
        	level			varchar(12)				not null,
        	created			timestamp default timezone('utc'::text, now())
        );
        ''',
        '''
        drop table if exists clickhouse_backup.backup_log;
        '''
    ),
    step(
        '''
        create table if not exists clickhouse_backup.tables_schema
        (
        	backup_guid		uuid					not null,
        	table_name		varchar(60)				not null,
        	field_name 		varchar(255)			not null,
        	field_type		varchar(255)			not null,
        	created			timestamp default timezone('utc'::text, now())
        );
        ''',
        '''drop table if exists clickhouse_backup.tables_schema;'''
    )
]