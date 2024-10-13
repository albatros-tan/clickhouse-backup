import click
import asyncio
from uuid import UUID

import migrations
from app.app import Backup, Restore, Listing
from settings.db import DbConfig


@click.group()
def cli():
    ...
    
    
@cli.command(help='apply migrations')
@click.option('--stop_on', type=str)
def migrate(stop_on):
    migrator = migrations.Migrator(dsn=DbConfig.CONNECTION_SETTINGS['dsn'], migration_path=DbConfig.PATH_TO_MIGRATIONS)
    migrator.apply(stop_on)


@cli.command(short_help='rollback migrations')
@click.option('--stop_on', type=str)
def rollback(stop_on):
    """Rollback to specified version."""
    migrator = migrations.Migrator(dsn=DbConfig.CONNECTION_SETTINGS['dsn'], migration_path=DbConfig.PATH_TO_MIGRATIONS)
    migrator.rollback(stop_on)


@cli.command(short_help='show not applied migrations')
def show_migrations():
    """Print to console not applied migrations."""
    migrator = migrations.Migrator(dsn=DbConfig.CONNECTION_SETTINGS['dsn'], migration_path=DbConfig.PATH_TO_MIGRATIONS)
    migrations_to_apply = migrator.get_migration_list(action=migrations.MigrationActions.apply)
    click.echo('Migrations to apply:')
    if migrations_to_apply:
        click.echo('\n'.join([f'\t{migration.path}' for migration in migrations_to_apply]))
    else:
        click.echo('All migrations is applied')
        
        
@cli.command(short_help='Create a new backup')
@click.option('--table', type=str, help="Create a new backup of the specified table")
@click.option("--force", type=bool, help="Create a new backup without checking for the existence of a similar record")
def backup(table: str = None, force: bool = None):
    app = Backup()
    asyncio.run(app.run(table=table, force=force))
    
    
@cli.command(short_help='Restore a backup')
@click.option('--table', type=str, help="Restore a backup to the specified table")
@click.option("--backup_guid", type=UUID, help="Restore a backup to the specified backup_guid")
def restore(table: str = None, backup_guid: UUID = None):
    app = Restore()
    asyncio.run(app.run(table=table, backup_guid=backup_guid))
    
    
@cli.command(short_help='Return the list of backups')
@click.option('--table', type=str, help="Return a list of backups for the specified table")
@click.option("--gt", type=str, help="Return a list of backups made after the specified date")
def listing(table: str = None, gt: str = None):
    app = Listing()
    asyncio.run(app.run(table=table, gt=gt))
    


if __name__ == '__main__':
    cli()

