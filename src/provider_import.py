#!/usr/local/bin/python

import luigi
import os
import shutil
import pandas as pd
import time
import pymysql


def stage_data(filename):
    """ Load data into stage table (pandas dataframe)

    :param filename: The name of file to import in shared drive
    :return: headers, df
    """
    xls_file = pd.ExcelFile(filename)
    df = xls_file.parse('Sheet1')
    return list(df), df


def get_connection(hostname, username, password):
    """ Get MySQL CNX

    :param hostname: Name of NAS drive to connect to
    :return: pymysql.connection
    """
    # connect to db
    connection = ''
    try:
        connection = pymysql.connect(host=hostname,
                                     user=username,
                                     password=password,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
    except pymysql.OperationalError:
        print("Failure")

    if connection:
        print("Success")

    return connection


def load_cpiproviderinfo(filename, dbName, username, password, headers, df):
    """ Load a list of providers into cpiproviderinfo table

    :param filename: Name of provider file
    :param dbName: DB name to load to
    :param headers: from stage_data()
    :param df: from stage_data()
    :return: void
    """
    cnx = get_connection(dbName, username, password)
    if 'A' in filename:
        try:
            with cnx.cursor() as cursor:
                sql = "INSERT INTO providerinfo(ENTY_NM, PROV_FST_NM, PROV_LST_NM, PROV_FULL_NM, PROV_NPI) " \
                      + " values (%s, %s, %s, %s, %s)"
                for index, row in df.iterrows():
                    cursor.execute(sql, (row['Type'], row['NpFirstName'], row['NpLastName'], row['NP'], row['NPI']))
                    cnx.commit()
        finally:
            cnx.close()
    if 'B' in filename:
        try:
            with cnx.cursor() as cursor:
                sql = "INSERT INTO providerinfo(ENTY_NM, PROV_FST_NM, PROV_LST_NM, PROV_FULL_NM, PROV_NPI) " \
                      + " values (%s, %s, %s, %s, %s)"
                for index, row in df.iterrows():
                    cursor.execute(sql, (
                    row['CDO'], row['Provider First Name'], row['Provider Last Name'], row['Provider Name'],
                    row['NPI']))
                    cnx.commit()
        finally:
            cnx.close()


class CreateLogFiles(luigi.Task):
    """ Root task to create XXXX_Errors.txt, XXXX_Duplicates.txt, and XXXX_DuplicatesExisting.txt

    """
    filename = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        timestr = time.strftime("%Y-%m-%d")
        return luigi.LocalTarget('/root/etc/mnt/Import/LogFiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + "_Duplicates.txt")

    def run(self):
        timestr = time.strftime("%Y-%m-%d")
        files = ['Errors.txt', 'Duplicates.txt', 'DuplicatesExisting.txt']
        for suffix in files:
            src_blank_file_str = '/root/etc/mnt/Import/LogFiles/Provider_Blank_DONOTDELETE.txt'
            dest_file_str = '/root/etc/mnt/Import/LogFiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_{}'.format(suffix)
            shutil.copyfile(src_blank_file_str, dest_file_str)


class LogErrors(luigi.Task):
    """ Count occurrences where provider full name is null. Logs erroneous rows and total count.

    """
    filename = luigi.Parameter()

    def requires(self):
        return CreateLogFiles(self.filename)

    def output(self):
        timestr = time.strftime("%Y-%m-%d")
        return luigi.LocalTarget('/root/etc/mnt/Import/' + os.path.splitext(self.filename)[0] + '_' + timestr +  '_Filtered.xlsx')

    def run(self):
        timestr = time.strftime("%Y-%m-%d")

        # load data
        headers, df = stage_data(self.filename)
        error_logfile_str = '/root/etc/mnt/Import/Logfiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_Errors.txt'

        # get errors (where NP fullname is null) and count
        idx_errors = df.index[df[headers[0]].isnull()]
        error_count = str(len(idx_errors))

        # log errors in 'XXXX_Errors.txt'
        with open(error_logfile_str, 'w') as out_file:
            print(df.loc[idx_errors], end='\n', file=out_file)
            print('Total Error Count ' + error_count, end='', file=out_file)

        # remove error rows and write to atomic task output
        df_new = df.drop(idx_errors)
        writer = pd.ExcelWriter('/root/etc/mnt/Import/' + os.path.splitext(self.filename)[0] + '_' + timestr +  '_Filtered.xlsx', engine='xlsxwriter')
        df_new.to_excel(writer, sheet_name='Sheet1')
        writer.save()

        #return error_count, df_new


class LogDuplicates(luigi.Task):
    """ Count occurrences where duplicate NPI numbers exist in the data to be loaded. Logs erroneous rows and total count.

    """
    filename = luigi.Parameter()

    def requires(self):
        return CreateLogFiles(self.filename)

    def output(self):
        timestr = time.strftime("%Y-%m-%d")
        return luigi.LocalTarget('/root/etc/mnt/Import/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_Filtered.xlsx')

    def run(self):
        timestr = time.strftime("%Y-%m-%d")

        # load data
        headers, df = stage_data(self.filename)
        duplicates_logfile_str = '/root/etc/mnt/Import/Logfiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_Duplicates.txt'

        # get errors (where NP fullname is null) and count
        idx_dups = df.index[df['NPI'].duplicated()]
        error_count = str(len(idx_dups))

        # log errors in 'XXXX_Errors.txt'
        with open(duplicates_logfile_str, 'w') as out_file:
            print(df.loc[idx_dups], end='\n', file=out_file)
            print('Total Duplicates Count ' + error_count, end='', file=out_file)

        # remove error rows and write to atomic task output
        df_new = df.drop(idx_dups)
        writer = pd.ExcelWriter('/root/etc/mnt/Import/' + os.path.splitext(self.filename)[0] + '_' + timestr +'_Filtered.xlsx',engine='xlsxwriter')
        df_new.to_excel(writer, sheet_name='Sheet1')
        writer.save()

        # return error_count, df_new


class ConnectDB(luigi.Task):
    """ Connect to database. Ouptus _DB_CNX_SUCCESS.txt flag if successfull

    """
    filename = luigi.Parameter()
    dbName = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        timestr = time.strftime("%Y-%m-%d")
        return luigi.LocalTarget('/root/etc/mnt/Import/LogFiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_DB_CNX_SUCCESS.txt')

    def run(self):
        timestr = time.strftime("%Y-%m-%d")
        connection = get_connection(self.dbName, self.username, self.password)
        src_blank_file_str = '/root/etc/mnt/Import/LogFiles/Provider_Blank_DONOTDELETE.txt'
        dest_file_str = '/root/etc/mnt/Import/LogFiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_DB_CNX_SUCCESS.txt'
        if connection:
            shutil.copyfile(src_blank_file_str, dest_file_str)


class LoadProviders(luigi.Task):
    filename = luigi.Parameter()
    dbName = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()

    def requires(self):
        # return ConnectDB(self.filename, self.dbName, self.username, self.password)
        return None

    def output(self):
        timestr = time.strftime("%Y-%m-%d")
        return luigi.LocalTarget('/root/etc/mnt/Import/LogFiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_PROV_LOAD_SUCCESS.txt')

    def run(self):
        timestr = time.strftime("%Y-%m-%d")
        headers, df = stage_data(self.filename)
        idx_dups = df.index[df['NPI'].duplicated()]
        df_new1 = df.drop(idx_dups)
        idx_errors = df_new1.index[df_new1[headers[0]].isnull()]
        df_new = df_new1.drop(idx_errors)
        load_cpiproviderinfo(self.filename, self.dbName, self.username, self.password, headers, df_new)
        src_blank_file_str = '/root/etc/mnt/Import/LogFiles/Provider_Blank_DONOTDELETE.txt'
        dest_file_str = '/root/etc/mnt/Import/LogFiles/' + os.path.splitext(self.filename)[0] + '_' + timestr + '_PROV_LOAD_SUCCESS.txt'
        shutil.copyfile(src_blank_file_str, dest_file_str)


class ImportFlow(luigi.WrapperTask):
    filename = luigi.Parameter()
    dbName = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()

    def run(self):
        print("Running Import for {}".format(self.filename))

    def requires(self):
        yield CreateLogFiles(self.filename)
        yield LogErrors(self.filename)
        yield LogDuplicates(self.filename)
        yield LoadProviders(self.filename, self.dbName, self.username, self.password)
        return

import random
if __name__ == '__main__':
    luigi.run()
