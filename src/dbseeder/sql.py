#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
sql.py
----------------------------------
sql queries shared between programs
'''
import pyodbc
from services import Reproject


sql_statements = {
    'station_insert': ('insert into Stations (OrgId, OrgName, StationId, StationName, StationType, StationComment,' +
                       ' HUC8, Lon_X, Lat_Y, HorAcc, HorAccUnit, HorCollMeth, HorRef, Elev, ElevUnit, ElevAcc,' +
                       ' ElevAccUnit, ElevMeth, ElevRef, StateCode, CountyCode, Aquifer, FmType, AquiferType,' +
                       ' ConstDate, Depth, DepthUnit, HoleDepth, HoleDUnit, demELEVm, DataSource, WIN, Shape)' +
                       ' values ({})'),
    'result_insert': ('insert into Results (AnalysisDate, AnalytMeth, AnalytMethId, AutoQual, CAS_Reg, Chrg,' +
                      ' DataSource, DetectCond, IdNum, LabComments, LabName, Lat_Y, LimitType, Lon_X, MDL,' +
                      ' MDLUnit, MethodDescript, OrgId, OrgName, Param, ParamGroup, ProjectId, QualCode,' +
                      ' ResultComment, ResultStatus, ResultValue, SampComment, SampDepth, SampDepthRef,' +
                      ' SampDepthU, SampEquip, SampFrac, SampleDate, SampleTime, SampleId, SampMedia, SampMeth,' +
                      ' SampMethName, SampType, StationId, Unit, USGSPCode) values ({})'),
}


def create_cursor(connection_string):
    c = pyodbc.connect(connection_string)
    return c.cursor()


def insert_rows(rows, insert_statement, cursor):
    '''Given a list of fields and a sql statement, execute the statement after `batch_size` number of statements'''
    batch_size = 5000

    i = 1
    #: format and stage sql statements
    for row in rows:
        statement = insert_statement.format(','.join(row))

        try:
            cursor.execute(statement)
            i += 1

            #: commit commands to database
            if i % batch_size == 0:
                cursor.commit()

            cursor.commit()
        except Exception, e:
            del cursor

            raise e


def update_row(row, datasource):
    '''Given a dictionary as a row, take the lat and long field, project it to UTM, and transform to WKT'''

    template = 'geometry::STGeomFromText(\'POINT ({} {})\', 26912)'

    row['DataSource'] = datasource

    x = row['Lon_X']
    y = row['Lat_Y']

    if not (x and y):
        return row

    if 'Shape' not in row:
        return row

    shape = Reproject.to_utm(x, y)

    row['Shape'] = template.format(shape[0], shape[1])

    return row
