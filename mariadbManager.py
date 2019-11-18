from contextlib import closing
import asyncio
import logging
import subprocess
import json
import importlib
from contextlib import closing
import argparse
import multiprocessing
from io import StringIO
import traceback
import sys
import time
from datetime import datetime
import os

import pymysql
import boto3

import argh
from argh import CommandError
import pymysql

from . import mariadbConfig

_logger = logging.getLogger(__name__)
schemaVersions = ['29', '30', '31', '32', '33', '34', '35']
loop = asyncio.get_event_loop()

async def connect(retryIntervalSecs=5, maxRetries=10):
    '''
    Create a new database connection. Will retry up to maxRetries times in
    intervals of retryIntervalSecs
    '''
    attempt = 0
    while True:
        try:
            # Connect
            conn = pymysql.connect(
                    host=mariadbConfig.host,
                    user=mariadbConfig.user,
                    passwd=mariadbConfig.password,
                    db=mariadbConfig.dbName,
                    use_unicode=True,
                    charset='utf8',
                    cursorclass=pymysql.cursors.DictCursor)
        except pymysql.OperationalError:
            if attempt == maxRetries:
                raise

            # Assume database is not up yet; sleep and retry
            logMsg = 'Could not connect to database %s, will try ' + \
                    'again after %s seconds...'
            _logger.info(logMsg, mariadbConfig.dbName, retryIntervalSecs)
            await asyncio.sleep(retryIntervalSecs)
            attempt += 1
        else:
            _logger.info('Connected to database %s successfully.',
                    mariadbConfig.dbName)
            break

    # Turn on autocommit
    conn.autocommit(True)

    # Set wait_timeout to its largest value (365 days): connection will be
    # disconnected only if it is idle for 365 days.
    with closing(conn.cursor()) as cursor:
        cursor.execute("SET wait_timeout=31536000")

    return conn


def connectSync(retryIntervalSecs=5, maxRetries=10):
    return asyncio.get_event_loop().run_until_complete(
            connect(retryIntervalSecs, maxRetries))


connection = None


def _backupOnAmazonS3(evt):
    s3ResourceClient = boto3.resource('s3')
    while True:
        ret = evt.wait(mariadbConfig.backupIntervalSecs)
        if ret:
            break
        _logger.info('Backing up MariaDB database %s to Amazon S3.',
                mariadbConfig.dbName)
        backupFilePath = None
        try:
            backupFileName = '{}-{}.sql'.format(mariadbConfig.dbName,
                    str(datetime.utcnow()))
            backupFilePath = '/tmp/' + backupFileName
            cmd = 'mysqldump ' \
                    f'--host={mariadbConfig.host} ' \
                    f'--user={mariadbConfig.user} ' \
                    f'--password="{mariadbConfig.password}" ' \
                    f'{mariadbConfig.dbName} > "{backupFilePath}"'
            subprocess.check_call(cmd, shell=True)
            if mariadbConfig.backupS3KeyPrefix:
                key = '{}/{}'.format(mariadbConfig.backupS3KeyPrefix,
                        backupFileName)
            else:
                key = backupFileName
            obj = s3ResourceClient.Object(
                    mariadbConfig.backupS3BucketName, key)
            obj.put(Body=open(backupFilePath, 'rb'),
                    ServerSideEncryption='AES256')
        except: # pylint: disable=bare-except
            tbStringIO = StringIO()
            traceback.print_exception(*sys.exc_info(), None, tbStringIO)
            _logger.error(tbStringIO.getvalue())
        finally:
            if backupFilePath:
                try:
                    os.remove(backupFilePath)
                except: # pylint: disable=bare-except
                    tbStringIO = StringIO()
                    traceback.print_exception(*sys.exc_info(), None, tbStringIO)
                    _logger.error(tbStringIO.getvalue())

        time.sleep(mariadbConfig.backupIntervalSecs)


class SchemaVersionMismatch(Exception):
    pass


def getSchemaVersion(metadataTableName='metadata'):
    '''
    Get currently installed database schema version.
    '''
    currentVersion = None
    with closing(connection.cursor()) as cursor:
        try:
            cursor.execute('SELECT * FROM ' + metadataTableName + \
                    ' WHERE attribute = %s', ('version',))
        except pymysql.ProgrammingError as e:
            # 1146 == table does not exist
            if e.args[0] == 1146:  # pylint: disable=no-else-return
                # Version 1 tables don't exist either, so it is most
                # likely that no schema is installed
                return None
            else:
                raise
        else:
            row = cursor.fetchone()
            if not row:
                raise CommandError('Could not read current ' +
                    'database version')
            currentVersion = row['value']

    return currentVersion


def _checkDbSchemaVersion():
    currentVersion = getSchemaVersion()

    # Get most recent version from config
    expectedVersion = schemaVersions[-1]

    if currentVersion != expectedVersion:
        raise SchemaVersionMismatch(
            ('Installed database schema version {} does '
            'not match expected version {}').format(
                currentVersion, expectedVersion))


_backupEvt = None
_backupProcess = None
_inited = False

async def init(checkSchemaVersion=True):
    # If already initialized, do nothing
    global _inited
    if _inited:
        return

    # Create default connection
    global connection
    connection = await connect()

    if checkSchemaVersion:
        # Check Database schema version matches what is expected
        _checkDbSchemaVersion()

    _inited = True


def initSync(checkSchemaVersion=True):
    asyncio.get_event_loop().run_until_complete(init())


def startPeriodicAmazonS3Backup():
    # Start periodic backup on Amazon S3
    _logger.info('Starting periodic MariaDB backup on Amazon S3.')
    global _backupEvt
    global _backupProcess
    _backupEvt = multiprocessing.Event()
    _backupProcess = multiprocessing.Process(
            target=_backupOnAmazonS3, args=(_backupEvt,))
    _backupProcess.start()


def cleanup():
    if _backupProcess is not None:
        _logger.info('Terminating MariaDB backup process')
        _backupEvt.set()
        _logger.info('MariaDB backup process terminated')


def makePlaceholderList(sourceList):
    return ','.join(['%s'] * len(sourceList)) or '""'


def askYesOrNoQuestion(question):
    cfm = input(question + ' (y/n): ')
    while cfm not in ('y', 'n'):
        cfm = input('Please type y or n: ')
    return cfm


def execute(sqlFilePath):
    '''
    Execute the SQL file at the given path.
    '''
    _logger.info('Executing SQL from file %s...', (sqlFilePath,))

    cfg = mariadbConfig
    cmd = 'mysql --host=%s --user=%s --password="%s" %s < %s' \
            % (cfg.host, cfg.user,
                    cfg.password, cfg.dbName,
                    sqlFilePath)
    subprocess.check_call(cmd, shell=True)

    _logger.info('Executed file %s successfully.', sqlFilePath)


@argh.arg('-l', '--logLevel',
        help='one of "debug", "info", "warning", "error", and "critical"')
@argh.arg('--logFormat',
        help='Python-like log format (see Python docs for details)')
def installSchema(logLevel='warning',
        logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
    '''
    Install the application's database schema.

    Warning: this will wipe out any existing database!
    '''
    logging.basicConfig(level=getattr(logging, logLevel.upper(),
        logging.NOTSET), format=logFormat)

    # Drop database if exists
    cmd = 'mysql --host=%s --user=%s --password="%s" \
            --execute="DROP DATABASE IF EXISTS \\`%s\\`"' \
            % (mariadbConfig.host, mariadbConfig.user,
                    mariadbConfig.password, mariadbConfig.dbName)
    subprocess.check_call(cmd, shell=True)

    _logger.info('Creating database...')

    # Create database
    cmd = 'mysql --host=%s --user=%s --password="%s" \
            --execute="CREATE DATABASE \\`%s\\`"' \
            % (mariadbConfig.host, mariadbConfig.user,
                    mariadbConfig.password, mariadbConfig.dbName)
    subprocess.check_call(cmd, shell=True)

    return execute(mariadbConfig.schemaFilePath)


@argh.arg('-l', '--logLevel',
        help='one of "debug", "info", "warning", "error", and "critical"')
@argh.arg('--logFormat',
        help='Python-like log format (see Python docs for details)')
def importData(jsonDataFilePath, ignoreConflicts=False,
        logLevel='warning',
        logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
    logging.basicConfig(level=getattr(logging, logLevel.upper(),
        logging.NOTSET), format=logFormat)

    # Create default connection
    loop.run_until_complete(init())

    from . import connection

    # Load data file
    tables = json.load(open(jsonDataFilePath, encoding='utf-8'))

    # Import
    for table in tables:
        _logger.info('Installing table %s', table['name'])
        for row in table['rows']:
            query = 'INSERT INTO ' + table['name'] + ' (' \
                    + ','.join(row.keys()) \
                    + ') VALUES (' + '%s' + ',%s' * (len(row) -1) + ')'
            with closing(connection.cursor()) as cursor:
                try:
                    cursor.execute(query, tuple(row.values()))
                except pymysql.IntegrityError:
                    if ignoreConflicts:
                        _logger.info('Ignoring conflict in table %s',
                                table['name'])
                        continue
                    else:
                        raise


@argh.arg('-t', '--targetVersion',
        help='target version for upgrade '
             '(default is latest version in Database package')
@argh.arg('-l', '--logLevel',
        help='one of "debug", "info", "warning", "error", and "critical"')
@argh.arg('--logFormat',
        help='Python-like log format (see Python docs for details)')
def upgrade(targetVersion=None, logLevel='warning',
        logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
    '''
    Perform a schema upgrade. An upgrade means a complete migration of
    data to the target version. It may be safer or more useful to
    wield the overlay-trim duo (as long as database size doesn't become
    an issue).

    This will call the application's target version's upgrade function.
    '''
    logging.basicConfig(level=getattr(logging, logLevel.upper(),
        logging.NOTSET), format=logFormat)

    # Create default connection
    loop.run_until_complete(init())

    currentVersion = getSchemaVersion()

    if currentVersion is None:
        raise CommandError('It seems there is no schema ' +
            'currently installed. You can install a schema with ' +
            'the "install" command.')
    elif currentVersion == targetVersion:
        raise CommandError('Schema version ' + currentVersion
                + ' is already installed')

    _logger.info('Current schema version = %s', currentVersion)

    # Default target version is the latest one available
    if targetVersion is None:
        targetVersion = schemaVersions[-1]

    # Get module for target version
    targetVersMod = importlib.import_module(
            f'.v{targetVersion}', 'CureCompanion.mariadb')

    # Make sure it has an upgrade function
    if not hasattr(targetVersMod, 'upgrade'):
        raise CommandError('Version ' + targetVersion +
                ' does not support ' +
                'the upgrade operation (hint: overlay and trim ' +
                'may be supported).')

    # Delegate to appropriate upgrade function
    targetVersMod.upgrade(fromVersion=currentVersion)


@argh.arg('-t', '--targetVersion',
        help='target version for upgrade '
             '(default is latest version in Database package')
@argh.arg('-l', '--logLevel',
        help='one of "debug", "info", "warning", "error", and "critical"')
@argh.arg('--logFormat',
        help='Python-like log format (see Python docs for details)')
def overlay(targetVersion=None,
        logLevel='warning',
        logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s',
        **kwargs):
    '''
    Overlay a schema on top of the currently installed one.

    Overlaying is a non-destructive, backwards-compatible first-step for
    schema migration. After overlaying, a superimposition of the current
    and target schemas will be installed. This is useful when both versions
    need to be simultaneously supported. When the previous version is no
    longer needed, perform a "trim" operation on the database.

    This will call the application's target version's overlay function.
    '''
    logging.basicConfig(level=getattr(logging, logLevel.upper(),
        logging.NOTSET), format=logFormat)

    # Create default connection
    loop.run_until_complete(init())

    currentVersion = getSchemaVersion()

    if currentVersion is None:
        raise CommandError('It seems there is no schema ' +
            'currently installed. You can install a schema with ' +
            'the "install" command.')
    elif currentVersion == targetVersion:
        raise CommandError('Schema version ' + currentVersion
                + ' is already installed')

    _logger.info('Current schema version = %s', currentVersion)

    # Default target version is the latest one available
    if targetVersion is None:
        targetVersion = schemaVersions[-1]

    # Get module for target version
    targetVersMod = importlib.import_module(
            f'.v{targetVersion}', 'CureCompanion.mariadb')

    # Make sure it has an overlay function
    if not hasattr(targetVersMod, 'overlay'):
        raise CommandError('Version ' + targetVersion +
                ' does support the overlay operation.')

    # Delegate to appropriate overlay function
    targetVersMod.overlay(fromVersion=currentVersion, **kwargs)


@argh.arg('-r', '--referenceVersion',
        help='reference version for trim operation')
@argh.arg('-t', '--trimVersion',
        help='version that is now obsolete')
@argh.arg('-l', '--logLevel',
        help='one of "debug", "info", "warning", "error", and "critical"')
@argh.arg('--logFormat',
        help='Python-like log format (see Python docs for details)')
def trim(referenceVersion=None, trimVersion=None,
        logLevel='warning',
        logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
    '''
    Trim the database to remove any data from a previous schema version
    that is now irrelevant to the reference schema version and thus
    obsolete.  This operation should be performed once the previous schema
    version no longer needs to be supported.

    This will call the application's reference version's trim function. If
    no reference version is specified, this will trim with reference to the
    currently installed schema version.
    '''
    logging.basicConfig(level=getattr(logging, logLevel.upper(),
        logging.NOTSET), format=logFormat)

    # Create default connection
    loop.run_until_complete(init())

    if referenceVersion is None:
        referenceVersion = getSchemaVersion()

    if referenceVersion is None:
        raise CommandError('It seems there is no schema ' +
            'currently installed.')

    # Get module for reference version
    refVersMod = importlib.import_module(
            f'.v{referenceVersion}', 'CureCompanion.mariadb')

    # Make sure it has a trim function
    if not hasattr(refVersMod, 'trim'):
        raise CommandError('Version ' + referenceVersion + ' does not ' +
            'support the trim operation.')

    # Set default trim version
    if trimVersion is None:
        trimVersion = refVersMod.previousVersion

    # Confirm with user
    response = askYesOrNoQuestion('Trim is a destructive and ' +
        'irreversible operation. Are you sure you want to proceed?!')

    if response == 'y':
        # Delegate to appropriate trim function
        refVersMod.trim(trimVersion)
    else:
        _logger.info('Trim not performed (whew!)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MariaDB database operations')
    argh.add_commands(parser, [execute, installSchema, importData,
        getSchemaVersion, upgrade, overlay, trim])
    argh.dispatch(parser)
