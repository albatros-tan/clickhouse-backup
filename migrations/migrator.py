import os
from enum import Enum
from logging import getLogger
from typing import Optional

from yoyo import get_backend
from yoyo.migrations import MigrationList, read_migrations, Migration

logger = getLogger(__name__)


class MigrationActions(Enum):
    apply = 'apply'
    rollback = 'rollback'
    all = 'all'


class Migrator(object):
    def __init__(self, dsn: str, migration_path: str):
        """Provide a simple interface for managing migrations."""
        self.database = get_backend(dsn)
        self.migrations = read_migrations(migration_path)

    def apply(self, stop_on: Optional[str] = None) -> None:
        migrations_list = self.get_migration_list(action=MigrationActions.apply, stop_on=stop_on)
        logger.debug('Apply migration list\n %s', '\n'.join([migration.path for migration in migrations_list]))
        with self.database.lock():
            self.database.apply_migrations(migrations_list)

    def rollback(self, stop_on: str = None) -> None:
        migrations_list = self.get_migration_list(action=MigrationActions.rollback, stop_on=stop_on)
        logger.debug('Rollback migration list %s', '\n'.join([migration.path for migration in migrations_list]))
        with self.database.lock():
            self.database.rollback_migrations(migrations_list)

    def get_migration_list(self, action: MigrationActions, stop_on: Optional[str] = None) -> MigrationList:
        migrations_list = self.migrations
        if action == MigrationActions.apply:
            migrations_list = self.database.to_apply(migrations_list)

        if action == MigrationActions.rollback:
            migrations_list = self.database.to_rollback(migrations_list)

        if not stop_on:
            return migrations_list

        for index, migration in enumerate(migrations_list):
            if os.path.basename(migration.path) == stop_on:
                return migrations_list[:index + 1]

        raise RuntimeError(f'Migration {stop_on} is not found in migrations list')

    def apply_one(self, name: str) -> None:
        """Apply one migration to database
        :param name: filename of migration.
        """
        migration_to_apply = self.get_migration(name)

        with self.database.lock():
            self.database.apply_one(migration_to_apply)
        logger.debug('Applied migration: %s', name)

    def rollback_one(self, name: str) -> None:
        """Rollback one migration from database
        :param name: filename of migration.
        """
        migration = self.get_migration(name, to_action=MigrationActions.rollback)

        with self.database.lock():
            self.database.rollback_one(migration)
        logger.debug('Migration: %s is rolled back', name)

    def get_migration(self, name: str, to_action: MigrationActions = MigrationActions.apply) -> Optional[Migration]:
        get_migrations = self.database.to_apply if to_action == MigrationActions.apply else self.database.to_rollback
        for migration in get_migrations(self.migrations):
            if os.path.basename(migration.path) == name:
                return migration

        raise RuntimeError(f'Cant find migration with name: {name}')
