"""dbss (Database Snapshot) -- Manage SQL Server Snapshots

SQL Server Database Snapshots preserve database state and allow us to revert
databases to baseline testing data easily (versus data loading from dump
files), see msdn for more information...
<http://msdn.microsoft.com/en-us/library/ms175158.aspx>

Database snapshots are only available in Enterprise and Development versions
of Microsoft SQL Server.

Use dbss.py to create a database snapshot, restore a database to its snapshot
or destroy a snapshot. 'test' will reveal the Transact-SQL statements used
(any database accepted, whereas other commands check against white list).

For environments: 'generate_baseline' captures databases. 'clean_slate' 
removes snapshots. 'revert_environment' restores databases via snapshots.

White lists are used to validate commands. The lists are environment specific,
use command 'list' to examine a desired white list.

kill_connections will kill client SQL SERVER connections, useful for testing.
Your connection pooling library may automatically reconnect though, so this
cannot be relied upon for reliable automation... 

Usage:
   dbss.py create (<database>) [--environment=<env>] [--quiet]
   dbss.py restore (<database>) [--environment=<env>] [--quiet]
   dbss.py destroy (<database>) [--environment=<env>] [--quiet]
   dbss.py test (<database>) [--environment=<env>]
   dbss.py list [--environment=<env>]
   dbss.py survey [--environment=<env>]
   dbss.py check_baseline [--environment=<env>]
   dbss.py kill_connections [--environment=<env>]
   dbss.py generate_baseline [--environment=<env>] [--quiet]
   dbss.py revert_environment [--environment=<env>] [--quiet]
   dbss.py clean_slate [--environment=<env>] [--quiet]
   dbss.py (-h | --help)
   dbss.py --version

Options:
   -h --help            Show help screen
   --version            Show version
   --environment=<env>  Environment (e.g. test, staging) [default: test]
   --quiet              Suppress narration [default: False]
"""

import docopt
import _mssql
import sys

# constants and sql-server conventions
# ... intial version taken from docopts examples
VERSION = '1.0.0rc2'


# configure environment
# ... do not use globals in functions, as they may
# ... be imported for use in other modules, use
# ... values stored in env to control behavior
def configure_environment(environment,quiet_mode):
    """Assign 'env' configuration for environment."""
    env = None
    if environment == 'test':
        db_list = ('CXSCORE', 'CXSERVER', 'IXDIRECTORY', 'IXDIRECTORY_PXQUOTE',
                   'IXDOC_CRU4', 'IXDOC_PXQUOTE_CRU4', 'IXLIBRARY_CRU4',
                   'IXLOG', 'IXLOGIC_CRU4', 'IXPROFILER', 'IXRELAY',
                   'IXVOCAB', 'PXCENTRAL_CRU4', 'PXGATEWAY_CRU4',
                   'PXPAY_CRU4', 'PXPOWER_CRU4', 'PXPROGRAM_CRU4',
                   'PXSERVER_CRU4', 'PXVAULT_CRU4')
        env = {}
        env['db_server'] = 'db_test_01'
        env['db_user'] = 'RedactedAppUser'
        env['db_pswd'] = 'edward_snowden'
        env['db_white_list'] = db_list
        env['quiet_mode'] = quiet_mode
        env['snapshot_suffix'] = '_dbss'
        env['snapshot_file_type'] = 'ss'
    return env


# string utility functions
def snapshot_name(db,env):
    """Convert database name to snapshot name."""
    dbss = db + env['snapshot_suffix']
    return dbss


def original_db_name(dbss,env):
    """Convert snapshot database name to source database name."""
    # use only if you *know* string is snapshot name
    last_char = -len(env['snapshot_suffix'])
    base_db = dbss[:last_char]
    return base_db


def is_snapshot(db,env):
    """Determine if database is a snapshot (based on name convention)."""
    if db.find(env['snapshot_suffix']) == -1:
        return False
    else:
        return True


# datbase interaction
# ... simple crud functions, build workflow into command functions
def sql_command(sql,env,err_code):
    """Execute SQL command statement."""
    db_result = 0
    dbs = env['db_server']
    dbu = env['db_user']
    dbp = env['db_pswd']
    quiet_mode = env['quiet_mode']
    try:
        connection = None
        connection = _mssql.connect(server=dbs,user=dbu,password=dbp)
        connection.execute_non_query(sql)
    except _mssql.MSSQLDatabaseException as e:
        db_result = err_code
        message = e.message
        first_period = message.find('.')
        message = message[:first_period]
        redundant_msg = message.find('DB-Lib error message')
        if redundant_msg > 2:
            message = message[:redundant_msg]
        if not quiet_mode:
            print(message)
        else:
            sys.stderr.write("[dbss/mssql] {}".format(message))
    finally:
        # note: if credentials refused, connection would be unbound
        # ... thus, assign to None, and test before closing
        if connection is not None:
            connection.close()
    if not db_result == 0:
        sys.exit(db_result)


def sql_query(sql,env,err_code):
    """Execute SQL query statement, return results as list of rows."""
    db_result = 0
    dbs = env['db_server']
    dbu = env['db_user']
    dbp = env['db_pswd']
    quiet_mode = env['quiet_mode']
    query_result = list()
    try:
        connection = None
        connection = _mssql.connect(server=dbs,user=dbu,password=dbp)
        connection.execute_query(sql)
        for row in connection:
            query_result.append(row)
    except _mssql.MSSQLDatabaseException as e:
        db_result = err_code
        message = e.message
        first_period = message.find('.')
        message = message[:first_period]
        if not quiet_mode:
            print(message)
        else:
            sys.stderr.write("[dbss/mssql] {}".format(message))
    finally:
        # note: if credentials refused, connection would be unbound
        # ... thus, assign to None, and test before closing
        if connection is not None:
            connection.close()
    if not db_result == 0:
        sys.exit(db_result)
    else:
        return query_result


def kill_connections(env):
    """Sever all database connections to allow snapshot restore."""
    db_result = 0
    err_code = 73
    this_spid = None
    spid_list = list()
    dbs = env['db_server']
    dbu = env['db_user']
    dbp = env['db_pswd']
    quiet_mode = env['quiet_mode']
    sql_kill_list = ''
    sql_this_connection = "select @@SPID;"
    sql_all_connections = "select spid from master.dbo.sysprocesses;"
    # note: spid up through 50 are reserved for sql server internals
    try:
        connection = None
        connection = _mssql.connect(server=dbs,user=dbu,password=dbp)
        connection.execute_query(sql_this_connection)
        for row in connection:
            this_spid = row[0]
        connection.execute_query(sql_all_connections)
        for row in connection:
            spid = row['spid']
            if (spid > 50) and (spid != this_spid):
                spid_list.append(spid)
        for spid in spid_list:
            sql_kill_list += "kill {};".format(spid)
        if sql_kill_list != '':
            connection.execute_non_query(sql_kill_list)
    except _mssql.MSSQLDatabaseException as e:
        db_result = err_code
        message = e.message
        first_period = message.find('.')
        message = message[:first_period]
        if not quiet_mode:
            print(message)
        else:
            sys.stderr.write("[dbss/mssql] {}".format(message))
    finally:
        # note: if credentials refused, connection would be unbound
        # ... thus, assign to None, and test before closing
        if connection is not None:
            connection.close()


def survey_databases(env,testing=False):
    """Obtain survey of databases (each with associated status)."""
    sql = "SELECT * FROM sys.databases;"
    if testing:
        print('   ' + sql)
        return
    server_survey = dict()
    query_result = sql_query(sql,env,85)
    for row in query_result:
        server_survey[row['name']] = row['state_desc']
    return server_survey


def survey_datafiles(db,env,testing=False):
    """Obtain survey of files associated with database."""
    sql  = "USE {}; ".format(db)
    sql += "SELECT * FROM sys.database_files WHERE type_desc<>'LOG';"
    if testing:
        print('   ' + sql)
        return
    server_file_list = list()
    query_result = sql_query(sql,env,79)
    for row in query_result:
        server_file = dict()
        server_file['name'] = row[5]
        server_file['filename'] = row[6]
        server_file_list.append(server_file)
    return server_file_list


def database_exists(db,env):
    """Determine if database exists in environment."""
    database_available = False
    if db in survey_databases(env):
        database_available = True
    return database_available


def capture_database(db,env,testing=False):
    """Create snapshot of database (core SQL command)."""
    snapshot_db = snapshot_name(db,env)
    snapshot_file_type = env['snapshot_file_type']
    if testing:
        # build simple create snapshot statement (example)
        file_dir  = r"D:\Program Files\Microsoft SQL Server"
        file_dir += r"\MSSQL10_50.MSSQLSERVER\MSSQL\DATA"
        logical_db = db + '_Data'
        snapshot_file = snapshot_db + '.' + snapshot_file_type
        dbss_path = file_dir + '\\' + snapshot_file
        sql  = "CREATE DATABASE {}".format(snapshot_db)
        sql += "\n      "
        sql += "ON ( NAME = {0}, FILENAME = '{1}' ) ".format(logical_db, dbss_path)
        sql += "\n      "
        sql += "AS SNAPSHOT OF {};".format(database)
        print('   ' + sql)
        return
    # build real-world snapshot statement
    # ... obtain list of database files for 'On' clause
    # adding "_dbss" to datafile name is not strictly
    # ... required as we are changing file extension
    # ... but this is a documented hack which adds
    # ... flexibility (for multiple snapshots)
    # alas, beware of case where database files
    # ... differ only by filename extension
    file_list = survey_datafiles(db,env)
    sql_file_list = ''
    file_count = 0
    for db_file in file_list:
        file_count += 1
        if sql_file_list != '':
            sql_file_list += ',\n'
        name = db_file['name']
        path_parts = db_file['filename'].split('.')
        path_parts[-2] += env['snapshot_suffix']
        if len(file_list) > 1:
            path_parts[-2] += '_' + str(file_count).zfill(2)
        path_parts[-1] = snapshot_file_type
        filename = '.'.join(path_parts)
        file_clause = "( NAME = {0}, FILENAME = '{1}' )".format(name,filename)
        sql_file_list += file_clause
    # build sql command
    sql  = "CREATE DATABASE {} ON".format(snapshot_db)
    sql += "\n" + sql_file_list + "\n"
    sql += "AS SNAPSHOT OF {};".format(database)
    sql_command(sql,env,86)


def restore_database(db,env,testing=False):
    """Revert database to snapshot (core SQL command)."""
    dbss = snapshot_name(db,env)
    sql  = "USE master; "
    sql += "RESTORE DATABASE {0} FROM DATABASE_SNAPSHOT = '{1}';".format(db,dbss)
    if testing:
        print('   ' + sql)
        return
    sql_command(sql,env,87)


def drop_database(db,env,testing=False):
    """Drop database (core SQL command, restricted to snapshots)."""
    # WARNING: destructive! command handler filters to be sure
    # ... only called on snapshot databases, but we add a
    # ... safety check -- remove if it hampers reuse
    sql = 'DROP DATABASE {};'.format(db)
    if testing:
        print('   ' + sql)
        return
    quiet_mode = env['quiet_mode']
    if not is_snapshot(db,env):
        message = "Request to drop {}, only snapshots may be dropped."\
                  .format(db)
        if not quiet_mode:
            print('Command failed: {}'.format(message))
        else:
            sys.stderr.write("dbss -- {}".format(message))
        sys.exit(84)
    sql_command(sql,env,88)


def drop_snapshot(db,env,testing=False):
    """Drop snapshot database."""
    snapshot_db = snapshot_name(db,env)
    drop_database(snapshot_db,env,testing)


def create_snapshot(db,env):
    """Create database snapshot."""
    # open question, what happens when you create snapshot
    # ... and one already exists? chose certainty
    quiet_mode = env['quiet_mode']
    snapshot_db = snapshot_name(db,env)
    if database_exists(snapshot_db,env):
        drop_database(snapshot_db,env)
    if database_exists(snapshot_db,env):
        message = 'Snapshot {} could not be dropped (for replacement).'\
                  .format(snapshot_db)
        if not quiet_mode:
            print('Command failed: {}'.format(message))
        else:
            sys.stderr.write("dbss -- {}".format(message))
        sys.exit(83)
    # check datbase status
    db_survey = survey_databases(env)
    db_status = db_survey[db]
    if db_status != 'ONLINE':
        message = "Database '{0}' is '{1}', status must be ONLINE for snapshot."\
                  .format(db,db_status)
        if not quiet_mode:
            print('Command failed: {}'.format(message))
        else:
            sys.stderr.write("dbss -- {}".format(message))
        sys.exit(77)
    # create snapshot
    capture_database(db,env)
    if not database_exists(snapshot_db,env):
        message = 'Snapshot {0} could not be created in {1}.'\
                  .format(snapshot_db,ENVIRONMENT)
        if not quiet_mode:
            print('Command failed: {}'.format(message))
        else:
            sys.stderr.write("dbss -- {}".format(message))
        sys.exit(82)


def restore_snapshot(db,env):
    """Revert database to snapshot."""
    snapshot_db = snapshot_name(db,env)
    if not database_exists(snapshot_db,env):
        message = 'Snapshot {0} does not exist in {1}.'\
                  .format(snapshot_db,ENVIRONMENT)
        if not QUIET_MODE:
            print('Command failed: {}'.format(message))
        else:
            sys.stderr.write("dbss -- {}".format(message))
        sys.exit(11)
    # check datbase status
    db_survey = survey_databases(env)
    db_status = db_survey[db]
    if db_status != 'ONLINE':
        message = "Database '{0}' is '{1}', status must be ONLINE for restore."\
                  .format(db,db_status)
        if not quiet_mode:
            print('Command failed: {}'.format(message))
        else:
            sys.stderr.write("dbss -- {}".format(message))
        sys.exit(78)
    # revert db to snapshot
    restore_database(db,env)


if __name__ == "__main__":
    # docopt handles non-command invocations (e.g help and version)
    config = docopt.docopt(__doc__, version=VERSION)

    # assemble required status
    TEST_MODE = config['test']
    QUIET_MODE = config['--quiet']
    ENVIRONMENT = config['--environment']
    database_required = config['destroy'] or config['create'] or config['restore']
    if database_required or TEST_MODE:
        database = config['<database>'].upper()
    else:
        database = '[none]'

    # adjust environment-relevant configs
    env = configure_environment(ENVIRONMENT, QUIET_MODE)

    # validate environment
    if env is None:
        message = "Environment '{}' Unknown".format(ENVIRONMENT)
        if not QUIET_MODE:
            print("Command failed: {}".format(message))
        else:
            sys.stderr.write("dbss -- {}".format(message))
        sys.exit(5)

    # validate database - ensure in white list
    if database_required:
        if database not in env['db_white_list']:
            message = "Database '{}' Unknown (check help for white list)"\
                      .format(database)
            if not QUIET_MODE:
                print("Command failed: {}".format(message))
            else:
                sys.stderr.write("dbss -- {}".format(message))
            sys.exit(6)

    # script interface
    if config['test']:
        print("SQL Statements used by dbss script...")
        print("\n1] Query to obtain databases in environment.")
        survey_databases(env,TEST_MODE)
        print("\n2] Query to obtain files associated with database.")
        survey_datafiles(database,env,TEST_MODE)
        print("\n3] Command to create database snapshot.")
        caveat  = "...[Warning: Create statement is purely example. "
        caveat += "File name and path must be built from query\n"
        caveat += "...on sys.database_files due to use "
        caveat += "of SQL Server filegroups. "
        caveat += "Filegroups should be kept online\n"
        caveat += "...to simplify snapshot use, "
        caveat += "and FILESTREAMS must be avoided.]"
        print(caveat)
        capture_database(database,env,TEST_MODE)
        print("\n4] Command to revert database to snapshot.")
        restore_database(database,env,TEST_MODE)
        print("\n5] Command to delete snapshot.")
        drop_snapshot(database,env,TEST_MODE)
        print("\n[finis]")
        # here is a good place to put items for temporary testing

    if config['list']:
        print("Database white list for {} environment:".format(ENVIRONMENT))
        for db in env['db_white_list']:
            print('  ' + db)
        print('  [finis]')

    if config['survey']:
        databases_available = survey_databases(env)
        database_list = databases_available.keys()
        database_list.sort()
        print("Survey of databases available in {} environment:".format(ENVIRONMENT))
        for db in database_list:
            print('  ' + db)
        print('  [finis - sanity checked!]')

    if config['check_baseline']:
        print("Checking snapshots available in {} environment against white list...".format(ENVIRONMENT))
        required_snapshots = list()
        databases_available = survey_databases(env)
        for db in env['db_white_list']:
            snapshot_db = snapshot_name(db,env)
            if not snapshot_db in databases_available:
                required_snapshots.append(db)
        if required_snapshots == []:
            print("Baseline ready (snapshots exist for required databases).")
        else:
            print("Snapshots missing for these databases:")
            print(required_snapshots)

    if config['kill_connections']:
        kill_connections(env)

    if config['create']:
        create_snapshot(database,env)
        if not QUIET_MODE:
            print('Snapshot created!')

    if config['restore']:
        # revert database to snapshot
        restore_snapshot(database,env)
        if not QUIET_MODE:
            print('Database restored!')

    if config['destroy']:
        snapshot_db = snapshot_name(database,env)
        # exit if snapshot does not exist
        if not database_exists(snapshot_db,env):
            if not QUIET_MODE:
                message = 'Snapshot {0} not found in {1}.'\
                          .format(snapshot_db,ENVIRONMENT)
                print(message)
            sys.exit(0)
        # drop snapshot
        drop_snapshot(database,env)
        if database_exists(snapshot_db,env):
            message = 'Snapshot {} could not be dropped'.format(snapshot_db)
            if not QUIET_MODE:
                print('Command failed: {}'.format(message))
            else:
                sys.stderr.write("dbss -- {}".format(message))
            sys.exit(81)
        if not QUIET_MODE:
            print('Snapshot destroyed!')

    if config['generate_baseline']:
        # generate baseline snapshots for environment
        for database in env['db_white_list']:
            if not QUIET_MODE:
                message = 'Creating snapshot for "{0}" in {1}.'\
                          .format(database, ENVIRONMENT)
                print(message)
            create_snapshot(database,env)
        if not QUIET_MODE:
            print('Environment baseline generated!')

    if config['revert_environment']:
        # revert environment databases to baseline
        for database in env['db_white_list']:
            if not QUIET_MODE:
                message = 'Restoring "{0}" from snapshot in {1}.'\
                          .format(database, ENVIRONMENT)
                print(message)
            restore_snapshot(database,env)
        if not QUIET_MODE:
            print('Environment reverted to baseline!')

    if config['clean_slate']:
        # drop snapshots from white_list in environment
        # differences exist in how 'clean_slate' and 
        # ... 'destroy' check existence of snapshot
        drop_list = list()
        dbss_list = list()
        # obtain snapshots, filter against white_list
        available_databases = survey_databases(env)
        for db in available_databases:
            if is_snapshot(db,env):
                dbss_list.append(db)
        for dbss in dbss_list:
            base_db = original_db_name(dbss,env)
            if base_db in env['db_white_list']:
                drop_list.append(dbss)
        if not QUIET_MODE:
            if drop_list == []:
                print('Slate clean (no snapshots to drop)')
                sys.exit(0)
            else:
                print('Database Snapshots to drop: ' + str(drop_list))
        # drop snapshots (with redundant error check)
        for dbss in drop_list:
            drop_database(dbss,env)
            if database_exists(dbss,env):
                message = 'Snapshot {} could not be dropped'.format(dbss)
                if not QUIET_MODE:
                    print('Command failed: {}'.format(message))
                else:
                    sys.stderr.write("dbss -- {}".format(message))
                sys.exit(80)
        if not QUIET_MODE:
            print('Snapshots dropped!')

    # program clean exit
    sys.exit(0)
