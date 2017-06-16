#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
services.py
----------------------------------
modules for acting on items
'''

import datetime
import re
from . import schema
from collections import OrderedDict
from csv import reader as csvreader
from dateutil.parser import parse
from .models import Concentration
from pyproj import Proj, transform
from requests import get
from .paramGroups import param_groups


class Reproject(object):
    '''A utility class for reprojecting points'''

    input_system = Proj(init='epsg:4326')
    ouput_system = Proj(init='epsg:26912')

    @classmethod
    def to_utm(cls, x, y):
        '''reproject x and y from 4326 to 26912'''

        if x > 0:
            x = x * -1

        return transform(cls.input_system, cls.ouput_system, x, y)


class Caster(object):
    '''A utility class for casting data to its defined schema type'''

    @classmethod
    def cast(cls, row, schema):
        '''Given a {string, string} dictionary (row) and the schema
        for the row (result or station) a new {string, string} dictionary
        (row) is returned with the values properly formatted.
        '''
        def noop(arg):
            return arg

        def date_conversion(arg):
            return datetime.time(*list(map(int, arg.split(':'))))

        for field in schema.items():
            cast = None
            #: if the value is not in the csv skip it
            try:
                value = row[field[0]]
            except KeyError:
                row[field[0]] = None
                continue

            #: try to remove all whitespace from strings
            try:
                value = value.strip()
            except:
                pass

            if field[1]['type'] == 'String':
                cast = str
            elif field[1]['type'] == 'Long Int':
                cast = int
            elif field[1]['type'] == 'Short Int':
                cast = int
            elif field[1]['type'] == 'Double':
                cast = float
            elif field[1]['type'] == 'Date':
                if isinstance(value, datetime.datetime):
                    cast = noop
                elif value == '':
                    row[field[0]] = None
                    continue
                else:
                    cast = parse
            elif field[1]['type'] == 'Time':
                if isinstance(value, datetime.time):
                    cast = noop
                elif value == '':
                    row[field[0]] = None
                    continue
                else:
                    cast = date_conversion

            try:
                value = cast(value)
                if (isinstance(value, datetime.datetime) and
                        (value > datetime.datetime.now() or
                            value.year < 1800)):
                    value = None
            except:
                row[field[0]] = None
                continue

            if value == '':
                row[field[0]] = None
                continue

            try:
                if field[1]['length']:
                    value = value[:field[1]['length']]
            except KeyError:
                pass
            except TypeError:
                #: you can't trim a datetime object
                #: but it has a length for some reason?
                pass

            try:
                if field[1]['actions']:
                    for action in field[1]['actions']:
                        value = locals()[action](value)
            except KeyError:
                pass

            row[field[0]] = value

        return row

    @classmethod
    def cast_for_sql(cls, row):
        '''
        Format the values for the sql insert statement
        '''

        for key in list(row.keys()):
            value = row[key]
            if key == 'Shape':
                continue
            elif isinstance(value, str):
                new_value = "'{}'".format(value.replace('\'', '\'\''))
            elif isinstance(value, datetime.datetime):
                try:
                    new_value = 'Cast(\'{}\' as date)'.format(value.strftime('%Y-%m-%d'))
                except ValueError:
                    #: year is before 1900
                    iso = value.isoformat()
                    date_string = iso.strip().split('T')[0]
                    new_value = 'Cast(\'{}\' as date)'.format(date_string)
            elif isinstance(value, datetime.time):
                new_value = "'{}'".format(str(value))
            elif value is None:
                new_value = 'Null'
            else:
                new_value = str(value)
            row[key] = new_value

        return row


class Normalizer(object):
    '''class for handling the normalization of fields'''

    station_types = {
        'Atmosphere': 'Atmosphere',
        'CERCLA Superfund Site': 'Other',
        'CG-2': 'Other',
        'Canal Drainage': 'Surface Water',
        'Canal Irrigation': 'Surface Water',
        'Canal Transport': 'Surface Water',
        'Cave': 'Other Groundwater',
        'Combined Sewer': 'Other',
        'Facility Industrial': 'Other',
        'Facility Municipal Sewage (POTW)': 'Other',
        'Facility Other': 'Other',
        'Facility Privately Owned Non-industrial': 'Facility',
        'Facility Public Water Supply (PWS)': 'Facility',
        'Facility: Cistern': 'Other',
        'Facility: Diversion': 'Facility',
        'Facility: Laboratory or sample-preparation area': 'Other',
        'Facility: Outfall': 'Other',
        'Facility: Storm sewer': 'Facility',
        'Facility: Waste injection well': 'Other',
        'Facility: Wastewater land application': 'Other',
        'Facility: Wastewater sewer': 'Other',
        'Facility: Water-distribution system': 'Facility',
        'GW': 'Other Groundwater',
        'Lake': 'Lake, Reservoir,  Impoundment',
        'Lake, Reservoir, Impoundment': 'Lake, Reservoir,  Impoundment',
        'Lake; Sediment Pond; Stagnant water': 'Lake, Reservoir,  Impoundment',
        'Land': 'Land',
        'Land Runoff': 'Land',
        'Land: Excavation': 'Land',
        'Land: Outcrop': 'Land',
        'Land: Sinkhole': 'Land',
        'MD': 'Other Groundwater',
        'Mine/Mine Discharge Adit (Mine Entrance)': 'Other Groundwater',
        'Other': 'Other',
        'Other-Ground Water': 'Other Groundwater',
        'Reservoir': 'Lake, Reservoir,  Impoundment',
        'River/Stream': 'Stream',
        'River/Stream Ephemeral': 'Stream',
        'River/Stream Perennial': 'Stream',
        'SP': 'Spring',
        'SW': 'Surface Water',
        'IN': 'Stream',
        'SS': 'Facility',
        'Seep': 'Spring',
        'Spring': 'Spring',
        'Storm Sewer': 'Other',
        'Stream': 'Stream',
        'Stream: Canal': 'Stream',
        'Stream: Ditch': 'Stream',
        'Subsurface: Cave': 'Other Groundwater',
        'Subsurface: Groundwater drain': 'Other Groundwater',
        'Subsurface: Tunnel, shaft, or mine': 'Other Groundwater',
        'UPDES Permit discharge point': 'Other Groundwater',
        'WL': 'Well',
        'Well': 'Well',
        'Well: Collector or Ranney type well': 'Well',
        'Well: Hyporheic-zone well': 'Well',
        'Well: Multiple wells': 'Well',
        'Well: Test hole not completed as a well': 'Well',
        'Wetland': 'Wetland',
        'Wetland Riverine-Emergent': 'Wetland'
    }

    samp_media = {
        'WL': 'Groundwater',
        'SP': 'Groundwater',
        'SS': 'Surface Water',
        'IN': 'Surface Water'
    }

    wqx_re = re.compile('(_WQX)-')

    @classmethod
    def strip_wxp(cls, id):
        return re.sub(cls.wqx_re, '-', id)

    @classmethod
    def normalize_sample(cls, row, datasource=None):
        '''In the units field, make all mg/L and ug/L lowercase while preserving
         other uppercase letters, convert units, normalize chemical
         strip wxp
        '''

        try:
            row['SampMedia'] = row['SampMedia'].strip()
            row['SampMedia'] = cls.samp_media[row['SampMedia']]
        except AttributeError:
            pass
        except KeyError:
            pass

        chemical = row['Param']
        unit = row['Unit']

        row['StationId'] = cls.strip_wxp(str(row['StationId']))

        if chemical is None:
            return row

        def calculate_amount(amount, conversion_rate):
            if amount is None:
                return None
            elif not amount:
                return 0

            return amount * conversion_rate

        def calculate_paramgroup(chemical):
            if chemical in param_groups:
                paramgroup = param_groups[chemical]

                return paramgroup

        inorganics_major_metals = ['calcium', 'dissolved calcium', 'dissolved magnesium', 'dissolved potassium', 'dissolved sodium', 'magnesium', 'potassium', 'sodium', 'sodium adsorption ratio',  # noqa
                                   'sodium adsorption ratio [(na)/(sq root of 1/2 ca + mg)]', 'sodium plus potassium', 'sodium, percent total cations', 'total calcium', 'total magnesium', 'total potassium', 'total sodium', 'percent sodium', 'hypochlorite ion']  # noqa
        inorganics_major_nonmetals = ['acidity as caco3', 'alkalinity', 'alkalinity, bicarbonate as caco3', 'alkalinity, carbonate as caco3', 'alkalinity, hydroxide as caco3', 'alkalinity, phenolphthalein (total hydroxide+1/2 carbonate)', 'alkalinity, total', 'alkalinity, total as caco3', 'bicarbonate', 'bicarbonate as caco3', 'bicarbonate as hco3', 'bromide', 'carbon dioxide', 'carbonate', 'carbonate (co3)', 'carbonate as caco3', 'carbonate as co3', 'chloride', 'chlorine', 'dissolved oxygen (do)', 'dissolved oxygen (field)', 'dissolved oxygen saturation', 'fluoride', 'fluorine', 'gran acid neutralizing capacity',  # noqa
                                      'hydrogen', 'hydrogen ion', 'hydroxide', 'inorganic carbon', 'oxygen', 'silica', 'silicon', 'sulfate', 'sulfide', 'sulfur', 'total alkalinity as caco3', 'total carbon', 'silica d/sio2', 't. alk/caco3', 'alkalinity as cac03', 'silica, dis. si02', 'carbon, total', 'chlorine dioxide', 'chlorite', 'residual chlorine', 'hydroxide as calcium carbonate', 'hydrogen sulfide', 'alkalinity, caco3 stability', 'acidity, total (caco3)', 'acidity, m.o. (caco3)', 'alkalinity, bicarbonate', 'alkalinity, carbonate', 'alkalinity, phenolphthalein', 'total chlorine', 'combined chlorine', 'perchlorate', 'free residual chlorine']  # noqa
        inorganics_minor_nonmetals = ['antimony', 'argon', 'arsenate (aso43-)', 'arsenic', 'arsenite', 'boron', 'bromine', 'cyanide', 'cyanides amenable to chlorination (hcn & cn)', 'dissolved arsenic', 'dissolved boron', 'dissolved selenium',  # noqa
                                      'germanium', 'helium', 'iodide', 'krypton', 'neon', 'perchlorate', 'selenium', 'sulfur hexafluoride', 'tellurium', 'total arsenic', 'total boron', 'total selenium', 'xenon', 'chlorate', 'antimony, total', 'boron, total', 'asbestos']  # noqa
        inorganics_minor_metals = ['aluminum', 'barium', 'beryllium', 'bismuth', 'cadmium', 'cerium', 'cesium', 'chromium', 'chromium(iii)', 'chromium(vi)', 'cobalt', 'copper', 'dissolved aluminum', 'dissolved barium', 'dissolved cadmium', 'dissolved chromium', 'dissolved copper', 'dissolved iron', 'dissolved lead', 'dissolved manganese', 'dissolved mercury', 'dissolved molybdenum', 'dissolved nickel', 'dissolved zinc', 'dysprosium', 'erbium', 'europium', 'gadolinium', 'gallium', 'holmium', 'iron', 'iron, ion (fe2+)', 'lanthanum', 'lead', 'lithium', 'lutetium', 'manganese', 'mercury', 'molybdenum', 'neodymium', 'nickel', 'niobium', 'praseodymium', 'rhenium', 'rubidium', 'samarium', 'scandium', 'silver',  # noqa
                                   'strontium', 'terbium', 'thallium', 'thulium', 'tin', 'titanium', 'total aluminum', 'total barium', 'total cadmium', 'total chromium', 'total copper', 'total iron', 'total iron-d max, dmr', 'total lead', 'total manganese', 'total mercury', 'total molybdenum', 'total nickel', 'total zinc', 'tungsten', 'vanadium', 'ytterbium', 'yttrium', 'zinc', 'zirconium', 'iron, dissolved', 'chromium, hex, as cr', 'copper, free', 'iron, suspended', 'manganese, suspended', 'beryllium, total', 'bismuth, total', 'chromium, hex', 'cobalt, total', 'lithium, total', 'molybdenum, total', 'thallium, total', 'tin, total', 'titanium, total', 'vanadium, total', 'lead summary', 'copper summary', 'manganese, dissolved']   # noqa
        nutrient = ['ammonia', 'ammonia and ammonium', 'ammonia as n', 'ammonia as nh3', 'ammonia-nitrogen', 'ammonia-nitrogen as n', 'ammonium', 'ammonium as n', 'dissolved nitrate: no3', 'inorganic nitrogen (nitrate and nitrite)', 'inorganic nitrogen (nitrate and nitrite) as n', 'kjeldahl nitrogen', 'nitrate', 'nitrate as n', 'nitrate-nitrogen', 'nitrite', 'nitrite as n', 'nitrogen', 'orthophosphate', 'nitrogen, ammonium/ammonia ratio', 'dissolved nitrite: no2', 'nitrogen, mixed forms (nh3), (nh4), organic, (no2) and (no3)',  # noqa
                    'no2+no3 as n', 'organic nitrogen', 'ortho. phosphate', 'orthophosphate as p', 'phosphate', 'phosphate-phosphorus', 'phosphate-phosphorus as p', 'phosphate-phosphorus as po4', 'phosphorus', 'total phosphorus', 'nitrate + nitrite as n', 'phosphate, tot. dig. (as p)', 't.k.n.', 'phosphorus 0 as p', 'nitrogen-ammonia as (n)', 'nitrate-nitrite', 'phosphate, total', 'total kjeldahl nitrogen (in water mg/l)', 'phosphorus, soluble', 'phosphate, reactive', 'phosphorus, total']  # noqa

        chemical = chemical.lower()
        milli_per_liter = 'mg/l'
        conversion_rate = None
        new_unit = None

        if (chemical in inorganics_major_metals or chemical in inorganics_major_nonmetals) and unit == 'ug/l':
            conversion_rate = 0.001
            new_unit = milli_per_liter
        elif ((chemical in inorganics_minor_metals or chemical in inorganics_minor_nonmetals) and
              unit == milli_per_liter):
            conversion_rate = 1000
            new_unit = 'ug/l'
        elif chemical in nutrient and unit == 'ug/l':
            conversion_rate = 0.001
            new_unit = milli_per_liter
        elif chemical == 'nitrate' and unit == 'mg/l as n':
            conversion_rate = 4.426802887
            new_unit = milli_per_liter
        elif chemical == 'nitrite' and unit == 'mg/l as n':
            conversion_rate = 3.284535258
            new_unit = milli_per_liter
        elif chemical == 'phosphate' and unit == 'mg/l as p':
            conversion_rate = 3.131265779
            new_unit = milli_per_liter
        elif chemical == 'carbonate as caco3' and unit == milli_per_liter:
            conversion_rate = 0.60
            new_unit = milli_per_liter
            chemical = 'carbonate'
        elif ((chemical == 'bicarbonate as caco3' and (unit == milli_per_liter or unit == 'mg/l as caco3')) or
              (chemical == 'alkalinity, bicarbonate as caco3' and unit == milli_per_liter)):
            conversion_rate = 1.22
            new_unit = milli_per_liter
            chemical = 'bicarbonate'
        elif chemical == 'alkalinity, carbonate' and unit == 'mg/l as caco3':
            conversion_rate = 0.60
            new_unit = milli_per_liter
            chemical = 'carbonate'
        elif (chemical == 'carbonate as co3' or chemical == 'carbonate (co3)') and unit == milli_per_liter:
            chemical = 'carbonate'
        elif chemical == 'bicarbonate as hco3' and unit == milli_per_liter:
            chemical = 'bicarbonate'
        elif chemical == 'alkalinity, carbonate as caco3' and unit == 'mg/l as caco3':
            conversion_rate = 0.60
            new_unit = milli_per_liter
            chemical = 'carbonate based on alkalinity'
        elif (((chemical == 'alkalinity, bicarbonate' or chemical == 'alkalinity') and unit == 'mg/l as caco3') or
              ((chemical == 't.alk/caco3' or chemical == 'total alkalinity as caco3') and unit == milli_per_liter)):
            conversion_rate = 1.22
            new_unit = milli_per_liter
            chemical = 'bicarbonate based on alkalinity'
        elif chemical == 'bicarbonate' and unit == 'mg/l as caco3':
            conversion_rate = 1.22
            new_unit = milli_per_liter
        elif chemical == 'phosphate-phosphorus' and (unit == 'mg/l as p' or unit == milli_per_liter):
            conversion_rate = 3.131265779
            new_unit = milli_per_liter
            chemical = 'phosphate'
        elif chemical == 'sulfate as s' and unit == milli_per_liter:
            conversion_rate = 0.333792756
            new_unit = milli_per_liter
            chemical = 'sulfate'
        elif chemical == 'nitrate' and unit == milli_per_liter and datasource == 'SDWIS':
            conversion_rate = 4.426802887
            new_unit = milli_per_liter
            chemical = 'nitrate'
        elif ((chemical == 'nitrate-nitrogen' and unit == 'mg/l as n') or
              (chemical == 'nitrate as n' and (unit == 'mg/l as n' or unit == milli_per_liter))):
            conversion_rate = 4.426802887
            new_unit = milli_per_liter
            chemical = 'nitrate'
        elif chemical == 'nitrate-nitrogen' and unit == milli_per_liter:
            conversion_rate = 4.426802887
            new_unit = milli_per_liter
            chemical = 'nitrite'
        elif chemical == 'nitrite as n' and (unit == 'mg/l as n' or unit == milli_per_liter):
            conversion_rate = 3.284535258
            new_unit = milli_per_liter
            chemical = 'nitrite'
        elif ((chemical == 'nitrate-nitrite' or
               chemical == 'inorganic nitrogen (nitrate and nitrite) as n' or
               chemical == 'nitrate + nitrate as n' or
               chemical == 'no2+no3 as n') and
              (unit == 'mg/l as n' or unit == milli_per_liter)):
            conversion_rate = 4.426802887
            new_unit = milli_per_liter
            chemical = 'nitrate and nitrite as no3'
        elif ((chemical == 'phosphate-phosphorus as p' and unit == 'mg/l as p') or
              (chemical == 'orthophosphate as p' and unit == 'mg/l as p') or
              (chemical == 'phosphate-phosphorus as p' and unit == milli_per_liter) or
              (chemical == 'orthophosphate as p' and unit == milli_per_liter) or
              (chemical == 'orthophosphate' and unit == 'mg/l as p')):
            conversion_rate = 3.131265779
            new_unit = milli_per_liter
            chemical = 'phosphate'
        elif chemical == 'ammonia and ammonium' and unit == 'mg/l nh4':
            conversion_rate = 1.05918619
            new_unit = milli_per_liter
            chemical = 'ammonia'
        elif ((chemical == 'ammonia-nitrogen as n' and unit == 'mg/l as n') or
              (chemical == 'ammonia-nitrogen' and unit == 'mg/l as n') or
              (chemical == 'ammonia-nitrogen as n' and unit == milli_per_liter) or
              (chemical == 'ammonia-nitrogen' and unit == milli_per_liter) or
              (chemical == 'ammonia' and unit == 'mg/l as n')):
            conversion_rate = 1.21587526
            new_unit = milli_per_liter
            chemical = 'ammonia'
        elif chemical == 'specific conductance' and unit == 'ms/cm':
            conversion_rate = 1000
            new_unit = 'uS/cm'
        elif chemical == 'specific conductance' and unit == 'umho/cm':
            new_unit = 'uS/cm'
        elif chemical == 'calcium' and unit == 'ueq/l':
            conversion_rate = 20.039
            new_unit = milli_per_liter
        elif chemical == 'magnesium' and unit == 'ueq/l':
            conversion_rate = 12.1525
            new_unit = milli_per_liter
        elif chemical == 'potassium' and unit == 'ueq/l':
            conversion_rate = 39.0983
            new_unit = milli_per_liter
        elif chemical == 'sodium' and unit == 'ueq/l':
            conversion_rate = 22.9897
            new_unit = milli_per_liter
        elif chemical == 'nitrate' and unit == 'ueq/l':
            conversion_rate = 62.0049
            new_unit = milli_per_liter
        elif chemical == 'chloride' and unit == 'ueq/l':
            conversion_rate = 35.453
            new_unit = milli_per_liter
        elif chemical == 'hydroxide' and unit == 'ueq/l':
            conversion_rate = 17.0073
            new_unit = milli_per_liter
        elif chemical == 'sulfate' and unit == 'ueq/l':
            conversion_rate = 24.01565
            new_unit = milli_per_liter

        row['Param'] = chemical

        if new_unit is not None:
            row['Unit'] = new_unit

        if conversion_rate is not None:
            row['ResultValue'] = calculate_amount(row['ResultValue'], conversion_rate)

        pgroup = calculate_paramgroup(chemical)
        if pgroup:
            row['ParamGroup'] = pgroup

        return row

    @classmethod
    def normalize_station(cls, row):
        '''strip wxp
        normalize fields
        '''
        row['StationId'] = cls.strip_wxp(str(row['StationId']))

        try:
            row['StationType'] = cls.station_types[row['StationType']]
        except KeyError:
            pass

        return row

    @classmethod
    def reorder_filter(cls, row, schema):
        new_row = OrderedDict()
        for field in schema:
            #: SDWIS is a partial schema
            #: add null values to missing fields
            if field not in row:
                row[field] = None

            new_row[field] = row[field]

        return new_row


class ChargeBalancer(object):
    '''https://github.com/agrc/ugs-chemistry/issues/22'''

    _conversions = {'ca': 0.04990269,
                    'mg': 0.082287595,
                    'na': 0.043497608,
                    'na+k': 0.043497608,
                    'k': 0.02557656,
                    'cl': 0.028206596,
                    'hco3': 0.016388838,
                    'co3': 0.033328223,
                    'so4': 0.020833333,
                    'no2': 0.021736513,
                    'no3': 0.016129032}

    @classmethod
    def calculate_charge_balance(cls, concentration, sampleId):
        calcium = cls._conversions['ca'] * (concentration.calcium or 0)
        magnesium = cls._conversions['mg'] * (concentration.magnesium or 0)
        sodium = cls._conversions['na'] * (concentration.sodium or 0)
        potassium = cls._conversions['k'] * (concentration.potassium or 0)
        chloride = cls._conversions['cl'] * (concentration.chloride or 0)
        bicarbonate = cls._conversions[
            'hco3'] * (concentration.bicarbonate or 0)
        sulfate = cls._conversions['so4'] * (concentration.sulfate or 0)
        carbonate = cls._conversions['co3'] * (concentration.carbonate or 0)
        nitrate = cls._conversions['no3'] * (concentration.nitrate or 0)
        nitrite = cls._conversions['no2'] * (concentration.nitrite or 0)
        sodium_plus_potassium = cls._conversions[
            'na+k'] * (concentration.sodium_plus_potassium or 0)

        cation = sum(
            [calcium, magnesium, sodium, potassium, sodium_plus_potassium])
        anion = sum(
            [chloride, bicarbonate, carbonate, sulfate, nitrate, nitrite])

        try:
            balance = 100 * float((cation - anion) / (cation + anion))
        except ZeroDivisionError:
            balance = 0

        def get_row(values):
            #: add all fields from result schema with None as the default value
            new_row = dict([(field, None) for field in list(schema.result.keys())])
            new_row.update(values)
            return new_row

        return [get_row({
            'SampleId': sampleId,
            'Param': 'Charge Balance',
            'ResultValue': round(balance, 2),
            'Unit': '%'
        }), get_row({
            'SampleId': sampleId,
            'Param': 'Cation Total',
            'ResultValue': round(cation, 2),
            'Unit': 'meq/l'
        }), get_row({
            'SampleId': sampleId,
            'Param': 'Anions Total',
            'ResultValue': round(anion, 2),
            'Unit': 'meq/l'
        })]

    @classmethod
    def get_charge_balance(cls, rows):
        con = Concentration()

        for r in rows:
            con.set(r['Param'], r['ResultValue'], r.get('DetectCond'))

        if con.has_major_params:
            return cls.calculate_charge_balance(con, rows[0]['SampleId'])
        else:
            return []


class HttpClient(object):
    """A wrapper around requests for testing"""

    @staticmethod
    def get_csv(url, logger):
        response = get(url)
        response.raise_for_status()

        try:
            logger.info('query completed in {}'.format(response.elapsed))
            logger.info('new sites found {}'.format(response.headers['total-site-count']))
            logger.info('new results found {}'.format(response.headers['total-result-count']))
        except:
            pass

        return csvreader((txt for txt in response.text.splitlines()))
