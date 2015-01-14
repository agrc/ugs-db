#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
services
----------------------------------

Classes that handle specific repeatable tasks.
"""
import datetime
import requests
import sys
from dateutil.parser import parse
from pyproj import Proj, transform


class WebQuery(object):

    """the http query wrapper over requests for unit testing"""
    web_api_url = ('http://api.mapserv.utah.gov/api/v1/search/{}/{}/'
                   '?geometry=point:[{},{}]&attributeStyle=upper&apikey={}')
    dev_api_key = 'AGRC-2D6BDFF7487510'

    tiger_county_url = ('http://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/'
                        'State_County/MapServer/94/query?geometry={},{}&geometryType=esriGeometryPoint&'
                        'inSR=26912&spatialRel=esriSpatialRelIntersects&outFields=county&'
                        'returnGeometry=false&f=json')

    wqp_results_url = ('http://www.waterqualitydata.us/Result/search?'
                       'sampleMedia=Water&startDateLo={}&startDateHi={}&'
                       'bBox=-115%2C35.5%2C-108%2C42.5&mimeType=csv')

    def results(self, date, url=None):
        if not url:
            url = self._format_wqp_result_url(date)

        r = requests.get(url)

        return r.text.splitlines()

    def _format_wqp_result_url(self, date, today=None):
        date_format = '%m-%d-%Y'
        most_recent_sample = date.strftime(date_format)

        if today:
            today = today.strftime(date_format)
        else:
            today = datetime.datetime.now().strftime(date_format)

        return self.wqp_results_url.format(most_recent_sample, today)

    def elevation(self, utm_x, utm_y):
        layer = 'SGID10.RASTER.DEM_10METER'
        attribute = 'VALUE'

        return self._query_web_api(layer, attribute, utm_x, utm_y)

    def state_code(self, utm_x, utm_y):
        layer = 'SGID10.BOUNDARIES.USSTATES'
        attribute = 'STATE_FIPS'

        return self._query_web_api(layer, attribute, utm_x, utm_y)

    def county_code(self, utm_x, utm_y):
        url = self.tiger_county_url.format(utm_x, utm_y)

        r = requests.get(url)

        response = r.json()

        if not response['features'] and response['features'].length < 1:
            raise LookupError('problem with the TIGER web service')

        return int(response['features'][0]['attributes']['COUNTY'])

    def _query_web_api(self, layer, attribute, x, y):
        url = self.web_api_url.format(layer,
                                      attribute,
                                      x,
                                      y,
                                      self.dev_api_key)

        r = requests.get(url)

        response = r.json()

        if response['status'] != 200:
            raise LookupError(response['message'])

        return int(response['result'][0]['attributes'][attribute])


class ConsolePrompt(object):

    def query_yes_no(self, question, default="yes"):
        """Ask a yes/no question via raw_input() and return their answer.

        "question" is a string that is presented to the user.
        "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

        The "answer" return value is one of "yes" or "no".
        """
        valid = {"yes": True,
                 "y": True,
                 "ye": True,
                 "no": False,
                 "n": False}
        if default is None:
            prompt = " [y/n] "
        elif default == "yes":
            prompt = " [Y/n] "
        elif default == "no":
            prompt = " [y/N] "
        else:
            raise ValueError("invalid default answer: '%s'" % default)

        while True:
            sys.stdout.write(question + prompt)
            choice = raw_input().lower()
            if default is not None and choice == '':
                return valid[default]
            elif choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'yes' or 'no' "
                                 "(or 'y' or 'n').\n")


class Project(object):

    input_system = Proj(init='epsg:4326')
    ouput_system = Proj(init='epsg:26912')

    min_x_wrong_sign = 100
    max_x_wrong_sign = 120

    def to_utm(self, x, y):
        if x > self.min_x_wrong_sign and x < self.max_x_wrong_sign:
            x = x * -1
        return transform(
            self.input_system,
            self.ouput_system,
            x,
            y)


class Caster(object):

    """takes argis row input and casts it to the defined schema type"""
    @staticmethod
    def cast(destination_value, destination_field_type):
        if destination_value is None:
            return None

        try:
            destination_value = destination_value.strip()
        except:
            pass

        if destination_field_type == 'TEXT':
            cast = str
        elif destination_field_type == 'LONG':
            cast = long
        elif destination_field_type == 'SHORT':
            cast = int
        elif (destination_field_type == 'FLOAT' or
              destination_field_type == 'DOUBLE'):
            cast = float
        elif destination_field_type == 'DATE':
            if isinstance(destination_value, datetime.datetime):
                cast = lambda x: x
            elif destination_value == '':
                return None
            else:
                cast = parse

        try:
            value = cast(destination_value)

            if value == '':
                return None

            return value
        except:
            return None


class Normalizer(object):

    """class for handling the normalization of fields"""

    def __init__(self):
        super(Normalizer, self).__init__()
        p = ('.alpha.-Endosulfan', '.alpha.-Hexachlorocyclohexane', '.beta.-Endosulfan', '.beta.-Hexachlorocyclohexane', '.delta.-Hexachlorocyclohexane', '.lambda.-Cyhalothrin', '1,1,1,2-Tetrachloroethane', '1,1,1-Trichloroethane', '1,1,2,2-Tetrachloroethane', '1,1,2-Trichloroethane', '1,1-Dichloroethane', '1,1-Dichloroethylene', '1,1-Dichloropropene', '1,2,3,4-Tetramethylbenzene', '1,2,3,5-Tetramethylbenzene', '1,2,3-Trichlorobenzene', '1,2,3-Trichloropropane', '1,2,3-Trimethylbenzene', '1,2,4-Trichlorobenzene', '1,2,4-Trimethylbenzene', '1,2-Dibromo-3-chloropropane', '1,2-Dichloroethane', '1,2-Dichloroethylene', '1,2-Dichloropropane', '1,3,5-Triazin-2(1H)-one, 4-(ethylamino)-6-[(1-methylethyl)amino]-', '1,3,5-Trimethylbenzene', '1,3-Dichloropropane', '1,4-Benzenedicarboxylic acid, 2,3,5,6-tetrachloro-, monomethyl ester', '1H-Benzotriazole, 5-methyl-', '1-Methylnaphthalene', '1-Naphthol', '1RS cis-Permethrin', '2,2,4,5,6,7,8,8-Octachloro-2,3,3a,4,7,7a-hexahydro-4,7-methano-1H-indene', '2,2-Dichloropropane', '2,4,5-Trichlorophenol', '2,4,5-T', '2,4,6-Trichlorophenol', '2,4-D methyl ester', '2,4-DB', '2,4-Dichlorophenol', '2,4-Dimethylphenol', '2,4-Dinitrophenol', '2,4-Dinitrotoluene', '2,4-D', '2,4-Pyrimidinediamine, 5-[(3,4,5-trimethoxyphenyl)methyl]-', '2,6-Diethylaniline', '2,6-Dimethylnaphthalene', '2,6-Dinitrotoluene', '2-Chloro-4,6-diamino-s-triazine', '2-Chloro-4-isopropylamino-6-amino-s-triazine', '2-Chloro-6-ethylamino-4-amino-s-triazine', '2-Chloroethyl vinyl ether', '2-Chloronaphthalene', '2-Ethyl-6-methylaniline', '2-Hexanone', '2-Methyl-2-butanol', '2-Methylnaphthalene', '2-Propanol, 1,3-dichloro-, phosphate (3:1)', '2-Pyrrolidinone, 1-methyl-5-(3-pyridinyl)-, (5S)-', '3,3,4,4,5,5-Hexachlorobiphenyl', '3,3-Dichlorobenzidine', '3,4-Dichloroaniline', '3,4-Dichlorophenyl isocyanate', '3,5-Dichloroaniline', '3-Hydroxycarbofuran', '3-Methylindole', '''4,4'-Isopropylidenediphenol''', '4-Chloro-2-methylphenol', '5-Amino-1-[2,6-dichloro-4-(trifluoromethyl)phenyl]-4-[(trifluoromethyl)thio]pyrazole-3-carbonitrile', 'Acenaphthene', 'Acenaphthylene', 'Acetaminophen', 'Acetochlor', 'Acetone', 'Acetophenone', 'Acifluorfen', 'Acrylonitrile', 'Age', 'Alachlor ESA', 'Alachlor', 'Aldicarb sulfone', 'Aldicarb sulfoxide', 'Aldicarb', 'Aldrin', 'Algae, floating mats (severity)', 'Alkalinity, Bicarbonate as CaCO3', 'Alkalinity, Carbonate as CaCO3', 'Alkalinity, Hydroxide as CaCO3', 'Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)', 'Alkalinity, total as CaCO3', 'Alkalinity, total', 'Alkalinity', 'Allyl chloride', 'Alpha particle', 'Aluminum', 'Aminomethylphosphonic acid', 'Ammonia and ammonium', 'Ammonia as NH3', 'Ammonia', 'Ammonia-nitrogen as N', 'Ammonia-nitrogen', 'Ammonium as N', 'Ammonium', 'Aniline', 'Anion deficit', 'Anthracene', 'Anthraquinone', 'Antimony', 'Apparent color', 'Argon', 'Aroclor 1016', 'Aroclor 1221', 'Aroclor 1232', 'Aroclor 1242', 'Aroclor 1248', 'Aroclor 1254', 'Aroclor 1260', 'Aroclor 1262', 'Arsenate (AsO43-)', 'Arsenic', 'Arsenite', 'Arsonic acid, methyl-, ion(1-)', 'Atrazine', 'Azinphos-methyl', 'Azobenzene', 'Barium', 'Barometric pressure', 'Bendiocarb', 'Benfluralin', 'Benomyl', 'Bensulfuron-methyl', 'Bentazon', 'Benz[a]anthracene', 'Benzene, 1,1-oxybis[2,4-dibromo-', 'Benzene, 1,3(and 1,4)-dimethyl- Chemical m(and p)-xylene', 'Benzene', 'Benzidine', 'Benzo(b)fluoranthene', 'Benzo[a]pyrene', 'Benzo[ghi]perylene', 'Benzo[k]fluoranthene', 'Benzoic acid', 'Benzophenone', 'Benzyl alcohol', 'Beryllium', 'Beta Cypermethrin', 'Beta particle', 'Bicarbonate', 'Biochemical oxygen demand, standard conditions', 'Biomass, periphyton', 'Bis(2-chloroethoxy)methane', 'Bis(2-chloroethyl) ether', 'Bis(2-chloroisopropyl) ether', 'Bismuth', 'Boron', 'Bromacil', 'Bromide', 'Bromine', 'Bromoacetic acid', 'Bromobenzene', 'Bromochloroacetic acid', 'Bromoxynil', 'Butachlor', 'Butyl benzyl phthalate', 'Butylate', 'C1-C3 Fluorenes', 'C1-C4 Chrysenes', 'C1-C4 Fluoranthenes', 'C1-C4 Phenanthrenes', 'Cacodylic acid', 'Cadmium', 'Caffeine', 'Calcium', 'Camphor', 'Carbaryl', 'Carbazole', 'Carbofuran', 'Carbon dioxide', 'Carbon disulfide', 'Carbon tetrachloride', 'Carbon-13/Carbon-12 ratio', 'Carbon-14', 'Carbonaceous biochemical oxygen demand, standard conditions', 'Carbonate (CO3)', 'Carbonate', 'Cerium', 'Cesium', 'CFC-113', 'CFC-11', 'CFC-12', 'Chemical oxygen demand', 'Chloramben-methyl', 'Chlordane', 'Chloride', 'Chlorimuron-ethyl', 'Chlorine', 'Chloroacetic acid', 'Chlorobenzene', 'Chlorodibromomethane', 'Chloroethane', 'Chloroform', 'Chloromethane', 'Chlorophyll a', 'Chlorophyll b', 'Chlorothalonil', 'Chlorpyrifos', 'Chlorthal-dimethyl', 'Cholestan-3-ol, (3.beta.,5.beta.)-', 'Cholesterol', 'Chromium(III)', 'Chromium(VI)', 'Chromium', 'cis-1,2-Dichloroethylene', 'cis-1,3-Dichloropropene', 'cis-Chlordane', 'Clopyralid', 'Cobalt', 'Conductivity', 'Copper', 'Cresol', 'Cumene', 'Cyanazine', 'Cyanide', 'Cyanides amenable to chlorination (HCN & CN)', 'Cycloate', 'Cyclohexane', 'Cyclohexene, 1-methyl-4-(1-methylethenyl)-, (4R)-', 'Cyclopenta[g]-2-benzopyran, 1,3,4,6,7,8-hexahydro-4,6,6,7,8,8-hexamethyl-', 'Cyfluthrin', 'Cymene', 'Dalapon', 'Dead fish, severity', 'Density of water at 20 deg C', 'Depth to water level below land surface', 'Depth, data-logger (non-ported)', 'Depth, data-logger (ported)', 'Depth, from ground surface to well water level', 'Depth, Secchi disk depth', 'Depth, snow cover', 'Depth', 'Detergent, severity', 'Deuterium/Hydrogen ratio', 'Di(2-ethylhexyl) adipate', 'Di(2-ethylhexyl) phthalate', 'Diazinon', 'Dibenz[a,h]anthracene', 'Dibenzofuran', 'Dibromoacetic acid', 'Dibromomethane', 'Dibutyl phthalate', 'Dicamba', 'Dichloroacetic acid', 'Dichlorobiphenyl', 'Dichlorobromomethane', 'Dichlorprop', 'Dichlorvos', 'Dicrotophos', 'Dieldrin', 'Diesel range organics', 'Diethyl phthalate', 'Dimethenamid', 'Dimethoate', 'Dimethyl phthalate', 'Dinitro-o-cresol', 'Di-n-octyl phthalate', 'Dinoseb', 'Diphenamid', 'Dissolved oxygen (DO)', 'Dissolved oxygen saturation', 'Disulfoton sulfone', 'Disulfoton', 'Diuron', 'Dysprosium', 'Elevation, water surface, MSL', 'Endosulfan sulfate', 'Endrin aldehyde', 'Endrin', 'Enterococcus', 'Erbium', 'Escherichia coli', 'Escherichia', 'Ethalfluralin', 'Ethanamine, 2-(diphenylmethoxy)-N,N-dimethyl-', 'Ethane', 'Ethanol, 2-(4-nonylphenoxy)-', 'Ethion monooxon', 'Ethion', 'Ethoprop', 'Ethyl ether', 'Ethyl methacrylate', 'Ethyl tert-butyl ether', 'Ethylbenzene', 'Ethylene dibromide', 'Ethylene glycol', 'Ethylene', 'Europium', 'Extended diesel range organics C10-C36', 'Fecal Coliform', 'Fecal coliforms', 'Fecal Streptococcus Group Bacteria', 'Fecal Streptococcus', 'Fenamiphos', 'Fenuron', 'Fipronil', 'Floating debris, severity', 'Flow rate, instantaneous', 'Flow', 'Flufenacet', 'Flumetsulam',
             'Fluometuron', 'Fluoranthene', 'Fluoride', 'Fluorine', 'Fonofos', 'Gadolinium', 'Gage height', 'Gallium', 'Gasoline range organics', 'Germanium', 'Glyphosate', 'Gran acid neutralizing capacity', 'Gross alpha radioactivity, (Thorium-230 ref std)', 'Gross beta radioactivity, (Cesium-137 ref std)', 'Halon 1011', 'Hardness, Ca, Mg as CaCO3', 'Hardness, Ca, Mg', 'Hardness, carbonate', 'Hardness, non-carbonate', 'Height, gage', 'Helium', 'Heptachlor epoxide', 'Heptachlorobiphenyl', 'Heptachlor', 'Hexachlorobenzene', 'Hexachlorobutadiene', 'Hexachlorocyclopentadiene', 'Hexachloroethane', 'Hexazinone', 'Holmium', 'Hydrocarbons, petroleum', 'Hydrogen ion', 'Hydrogen', 'Hydroxide', 'Imazaquin', 'Imazethapyr', 'Imidacloprid', 'Indeno[1,2,3-cd]pyrene', 'Indole', 'Inorganic carbon', 'Inorganic nitrogen (nitrate and nitrite) as N', 'Inorganic nitrogen (nitrate and nitrite)', 'Instream features, est. stream width', 'Iodide', 'Iprodione', 'Iron, ion (Fe2+)', 'Iron', 'Isoborneol', 'Isofenphos', 'Isophorone', 'Isopropyl ether', 'Isoquinoline', 'Kjeldahl nitrogen', 'Krypton', 'Langelier index (pHs)', 'Lanthanum', 'Lead', 'Lindane', 'Linuron', 'Lithium', 'Lutetium', 'Magnesium', 'Malaoxon', 'Malathion', 'Manganese', 'MBAS', 'MCPA', 'MCPB', 'm-Dichlorobenzene', 'Mercury', 'meta & para Xylene mix', 'Metalaxyl', 'Methacrylonitrile', 'Methane', 'Methidathion', 'Methiocarb', 'Methomyl', 'Methoxychlor', 'Methyl acetate', 'Methyl acrylate', 'Methyl bromide', 'Methyl ethyl ketone', 'Methyl iodide', 'Methyl isobutyl ketone', 'Methyl methacrylate', 'Methyl paraoxon', 'Methyl parathion', 'Methyl salicylate', 'Methyl tert-butyl ether', 'Methylene chloride', 'Methylmercury(1+)', 'Metolachlor', 'Metribuzin', 'Metsulfuron-methyl', 'm-Nitroaniline', 'Molinate', 'Molybdenum', 'Morphinan-6-ol, 7,8-didehydro-4,5-epoxy-3-methoxy-17-methyl-, (5.alpha.,6.alpha.)-', 'Myclobutanil', 'N,N-Diethyl-m-toluamide', 'Naphthalene', 'Napropamide', 'n-Butylbenzene', 'Neburon', 'Neodymium', 'Neon', 'Nickel', 'Nicosulfuron', 'Niobium', 'Nitrate as N', 'Nitrate', 'Nitrate-Nitrogen', 'Nitrite as N', 'Nitrite', 'Nitrobenzene', 'Nitrogen, ammonium/ammonia ratio', 'Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)', 'Nitrogen-15/14 ratio', 'Nitrogen', 'N-Nitrosodimethylamine', 'N-Nitrosodi-n-propylamine', 'N-Nitrosodiphenylamine', 'Norflurazon', 'n-Propylbenzene', 'o-Chlorophenol', 'o-Chlorotoluene', 'o-Cresol', 'Octachlorobiphenyl', 'o-Dichlorobenzene', 'Odor threshold number', 'Odor, atmospheric', 'o-Ethyltoluene', 'Oil and grease -- CWA 304B', 'Oil and grease', 'o-Nitroaniline', 'o-Nitrophenol', 'Organic anions', 'Organic carbon', 'Organic nitrogen', 'Orthophosphate as P', 'Orthophosphate', 'Oryzalin', 'Oxamyl', 'Oxidation reduction potential (ORP)', 'Oxyfluorfen', 'Oxygen', 'Oxygen-18/Oxygen-16 ratio', 'o-Xylene', 'p-(1,1,3,3-Tetramethylbutyl)phenol', 'p,p-DDD', 'p,p-DDE', 'p,p-DDT', 'Parathion', 'Partial pressure of dissolved gases', 'Particle size', 'Particle size, Sieve No. 230, 250 mesh, (0.063mm)', 'p-Bromophenyl phenyl ether', 'p-Chloroaniline', 'p-Chloro-m-cresol', 'p-Chlorophenyl phenyl ether', 'p-Chlorotoluene', 'p-Cresol', 'p-Cymene', 'p-Dichlorobenzene', 'Pebulate', 'Pendimethalin', 'Pentachlorobiphenyl', 'Pentachlorophenol', 'Perchlorate', 'pH, lab', 'Phenanthrene', 'Phenol, 2-(1,1-dimethylethyl)-4-methoxy-', 'Phenol, 4-(1-methyl-1-phenylethyl)-', 'Phenol', 'Pheophytin a', 'pH', 'Phorate', 'Phosmetoxon', 'Phosmet', 'Phosphate', 'Phosphate-phosphorus as P', 'Phosphate-phosphorus as PO4', 'Phosphate-phosphorus', 'Phosphoric acid, diethyl 6-methyl-2-(1-methylethyl)-4-pyrimidinyl ester', 'Phosphorus', 'Picloram', 'p-Nitroaniline', 'p-Nitrophenol', 'p-Octylphenol', 'Potassium', 'Praseodymium', 'Precipitation', 'Prometon', 'Prometryn', 'Pronamide', 'Propachlor', 'Propanil', 'Propargite', 'Propham', 'Propiconazole', 'Propoxur', 'Propylene glycol allyl ether', 'Pyrene', 'Radium-226', 'Radium-228', 'Radon-222', 'RBP Stream Depth - Run', 'RBP Stream Velocity', 'RBP2, Instream features, sampling reach area', 'Reservoir volume', 'Rhenium', 'Rubidium', 'Salinity', 'Samarium', 'Scandium', 'sec-Butylbenzene', 'Sediment', 'Selenium', 'S-Ethyl dipropylthiocarbamate', 'Siduron', 'Silica', 'Silicon', 'Silver', 'Silvex', 'Simazine', 'Sodium adsorption ratio [(Na)/(sq root of 1/2 Ca + Mg)]', 'Sodium adsorption ratio', 'Sodium plus potassium', 'Sodium, percent total cations', 'Sodium', 'Specific conductance', 'Specific conductivity', 'Specific gravity', 'Stigmast-5-en-3-ol, (3.beta.)-', 'Stigmastan-3-ol, (3.beta.)-', 'Stream flow, instantaneous', 'Stream flow, mean. daily', 'Stream width measure', 'Strontium-87/strontium-86, ratio', 'Strontium', 'Styrene', 'Sulfamethoxazole', 'Sulfate as S', 'Sulfate as SO4', 'Sulfate', 'Sulfide', 'Sulfometuron methyl', 'Sulfur hexafluoride', 'Sulfur-34/Sulfur-32 ratio', 'Sulfur', 'Sum of anions', 'Sum of cations', 'Suspended sediment concentration (SSC)', 'Suspended sediment discharge', 'Tebuthiuron', 'Tefluthrin', 'Tellurium', 'Temperature, air', 'Temperature, sample', 'Temperature, water', 'Terbacil', 'Terbium', 'Terbufos', 'Terbuthylazine', 'tert-Amyl methyl ether', 'tert-Butanol', 'tert-Butylbenzene', 'Tetrachlorobiphenyl', 'Tetrachloroethylene', 'Tetrahydrofuran', 'Thallium', 'Thiabendazole', 'Thiobencarb', 'Thorium-232', 'Thulium', 'Tin', 'Titanium', 'Toluene', 'Total Carbon', 'Total Coliform', 'Total coliforms', 'Total dissolved solids', 'Total fixed solids', 'Total hardness -- SDWA NPDWR', 'Total Sample Volume', 'Total suspended solids', 'Total volatile solids', 'Toxaphene', 'trans-1,2-Dichloroethylene', 'trans-1,3-Dichloropropene', 'trans-1,4-Dichloro-2-butene', 'trans-Nonachlor', 'Trash, Debris, Floatables', 'Triallate', 'Tribenuron-methyl', 'Tribromomethane', 'Tribufos', 'Tributyl phosphate', 'Trichloroacetic acid', 'Trichlorobiphenyl', 'Trichloroethylene', 'Triclopyr', 'Triclosan', 'Triethyl citrate', 'Trifluralin', 'Trihalomethanes (four), total, from SDWA NPDWR', 'Trihalomethanes', 'Triphenyl phosphate', 'Tris(2-butoxyethyl) phosphate', 'Tris(2-chloroethyl) phosphate', 'Tritium', 'True color', 'Tungsten', 'Turbidity severity', 'Turbidity', 'Uranium-234 and/or uranium-235 and/or uranium-238', 'Uranium-234/235/238', 'Uranium-234', 'Uranium-235', 'Uranium-238', 'Uranium', 'UV 254 -- SDWA NPDWR', 'Vanadium', 'Velocity - stream', 'Velocity-discharge', 'Vinyl bromide', 'Vinyl chloride', 'Volume Storage', 'Warfarin', 'Water content of snow', 'Water level in well, depth from a reference point', 'Water transparency, Secchi disc', 'Water', 'Wave height', 'Width', 'Wind direction (direction from, expressed 0-360 deg)', 'Wind velocity', 'Xenon', 'Xylenes mix of m + o + p', 'Xylene', 'Ytterbium', 'Yttrium', 'Zinc', 'Zirconium', 'Zooplankton')
        q = ('Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Toxicity', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Toxicity', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, PCBs', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Radiochemical', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Physical', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Organics, pesticide', 'Radiochemical', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Nutrient', 'Nutrient', 'Nutrient', 'Nutrient', 'Nutrient', 'Nutrient', 'Nutrient', 'Organics, other', '', 'Organics, other', 'Organics, other', 'Inorganics, Minor, Non-metals', 'Physical', 'Inorganics, Minor, Non-metals', 'Organics, PCBs', 'Organics, PCBs', 'Organics, PCBs', 'Organics, PCBs', 'Organics, PCBs', 'Organics, PCBs', 'Organics, PCBs', 'Organics, PCBs', 'Inorganics, Minor, Non-metals', 'Inorganics, Minor, Non-metals', 'Inorganics, Minor, Non-metals', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Physical', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Toxicity', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Radiochemical', 'Inorganics, Major, Non-metals', 'Physical', 'Biological', 'Organics, other', 'Organics, other', 'Organics, other', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Non-metals', 'Organics, pesticide', 'Inorganics, Major, Non-metals', 'Inorganics, Minor, Non-metals', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', '', '', '', '', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Organics, other', 'Inorganics, Major, Metals', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Major, Non-metals', 'Organics, pesticide', 'Toxicity', 'Stable Isotopes', 'Radiochemical', 'Physical', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Organics, other', 'Organics, other', 'Organics, other', 'Physical', 'Organics, pesticide', '', 'Inorganics, Major, Non-metals', 'Organics, pesticide', 'Inorganics, Major, Non-metals', 'Organics, other', 'Toxicity', 'Organics, other', 'Organics, other', 'Toxicity', 'Organics, other', 'Biological', 'Biological', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Physical', 'Inorganics, Minor, Metals', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Inorganics, Minor, Non-metals', 'Inorganics, Minor, Non-metals', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Biological', 'Physical', 'Physical', 'Information', 'Information', 'Physical', 'Physical', 'Physical', 'Information', 'Physical', 'Stable Isotopes', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Physical', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Microbiological', 'Inorganics, Minor, Metals', 'Microbiological', 'Microbiological', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Inorganics, Minor, Metals', 'Organics, other', 'Microbiological', 'Microbiological', 'Microbiological', 'Microbiological', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Physical', 'Physical', 'Physical', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Physical', 'Inorganics, Minor, Metals', 'Organics, other', 'Inorganics, Minor, Non-metals',
             'Organics, pesticide', '', 'Radiochemical', 'Radiochemical', 'Organics, other', 'Physical', 'Physical', 'Physical', 'Physical', 'Physical', 'Inorganics, Minor, Non-metals', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Organics, other', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Inorganics, Major, Non-metals', 'Nutrient', 'Nutrient', 'Physical', 'Inorganics, Minor, Non-metals', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Nutrient', 'Inorganics, Minor, Non-metals', '', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Inorganics, Major, Metals', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Inorganics, Minor, Metals', '', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Toxicity', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Non-metals', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Nutrient', 'Nutrient', 'Nutrient', 'Nutrient', 'Nutrient', 'Organics, other', 'Nutrient', 'Nutrient', 'Stable Isotopes', 'Nutrient', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, PCBs', 'Organics, other', 'Physical', 'Physical', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Nutrient', 'Nutrient', 'Nutrient', 'Organics, pesticide', 'Organics, pesticide', 'Physical', 'Organics, pesticide', 'Inorganics, Major, Non-metals', 'Stable Isotopes', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Physical', 'Sediment', 'Sediment', 'Organics, other', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Toxicity', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Minor, Non-metals', 'Physical', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Biological', 'Physical', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Nutrient', 'Nutrient', 'Nutrient', 'Nutrient', 'Organics, pesticide', 'Nutrient', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Inorganics, Major, Metals', 'Inorganics, Minor, Metals', 'Physical', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', 'Organics, pesticide', '', 'Organics, other', 'Radiochemical', 'Radiochemical', 'Radiochemical', '', '', '', '', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Physical', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Organics, other', 'Sediment', 'Inorganics, Minor, Non-metals', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Major, Metals', 'Inorganics, Major, Metals', 'Inorganics, Major, Metals', 'Inorganics, Major, Metals', 'Inorganics, Major, Metals', 'Physical', 'Physical', 'Physical', 'Organics, other', 'Organics, other', 'Physical', 'Physical', 'Physical', 'Stable Isotopes', 'Inorganics, Minor, Metals', 'Organics, other', 'Organics, other', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Inorganics, Major, Non-metals', 'Inorganics, Major, Non-metals', 'Organics, pesticide', 'Inorganics, Minor, Non-metals', 'Stable Isotopes', 'Inorganics, Major, Non-metals', '', '', 'Sediment', 'Sediment', 'Organics, pesticide', 'Organics, pesticide', 'Inorganics, Minor, Non-metals', 'Physical', 'Physical', 'Physical', 'Organics, pesticide', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Toxicity', 'Organics, other', 'Inorganics, Minor, Metals', 'Organics, pesticide', 'Organics, pesticide', 'Radiochemical', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Organics, other', 'Inorganics, Major, Non-metals', 'Microbiological', 'Microbiological', 'Physical', 'Physical', 'Physical', 'Information', 'Physical', 'Physical', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Physical', 'Organics, pesticide', 'Organics, pesticide', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Toxicity', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, pesticide', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Organics, other', 'Radiochemical', 'Physical', 'Inorganics, Minor, Metals', 'Physical', 'Physical', 'Radiochemical', 'Radiochemical', 'Radiochemical', 'Radiochemical', 'Radiochemical', 'Radiochemical', 'Physical', 'Inorganics, Minor, Metals', 'Physical', 'Physical', 'Organics, other', 'Toxicity', 'Physical', 'Organics, other', 'Physical', 'Physical', 'Physical', 'Physical', 'Physical', 'Information', 'Physical', 'Physical', 'Inorganics, Minor, Non-metals', 'Organics, other', 'Organics, other', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Inorganics, Minor, Metals', 'Biological')

        self.paramgroup = dict(zip(p, q))

    def normalize_unit(self, chemical, unit, current_amount):
        """
        In the units field, make all mg/L and ug/L lowercase while preserving
         other uppercase letters
        convert units
        normalize chemical

        """

        if chemical is None:
            return current_amount, unit, chemical

        inorganics_major_metals = ['calcium', 'dissolved calcium', 'dissolved magnesium', 'dissolved potassium', 'dissolved sodium', 'magnesium', 'potassium', 'sodium', 'sodium adsorption ratio',
                                   'sodium adsorption ratio [(na)/(sq root of 1/2 ca + mg)]', 'sodium plus potassium', 'sodium, percent total cations', 'total calcium', 'total magnesium', 'total potassium', 'total sodium', 'percent sodium', 'hypochlorite ion']
        inorganics_major_nonmetals = ['acidity as caco3', 'alkalinity', 'alkalinity, bicarbonate as caco3', 'alkalinity, carbonate as caco3', 'alkalinity, hydroxide as caco3', 'alkalinity, phenolphthalein (total hydroxide+1/2 carbonate)', 'alkalinity, total', 'alkalinity, total as caco3', 'bicarbonate', 'bicarbonate as caco3', 'bicarbonate as hco3', 'bromide', 'carbon dioxide', 'carbonate', 'carbonate (co3)', 'carbonate as caco3', 'carbonate as co3', 'chloride', 'chlorine', 'dissolved oxygen (do)', 'dissolved oxygen (field)', 'dissolved oxygen saturation', 'fluoride', 'fluorine', 'gran acid neutralizing capacity',
                                      'hydrogen', 'hydrogen ion', 'hydroxide', 'inorganic carbon', 'oxygen', 'silica', 'silicon', 'sulfate', 'sulfide', 'sulfur', 'total alkalinity as caco3', 'total carbon', 'silica d/sio2', 't. alk/caco3', 'alkalinity as cac03', 'silica, dis. si02', 'carbon, total', 'chlorine dioxide', 'chlorite', 'residual chlorine', 'hydroxide as calcium carbonate', 'hydrogen sulfide', 'alkalinity, caco3 stability', 'acidity, total (caco3)', 'acidity, m.o. (caco3)', 'alkalinity, bicarbonate', 'alkalinity, carbonate', 'alkalinity, phenolphthalein', 'total chlorine', 'combined chlorine', 'perchlorate', 'free residual chlorine']
        inorganics_minor_nonmetals = ['antimony', 'argon', 'arsenate (aso43-)', 'arsenic', 'arsenite', 'boron', 'bromine', 'cyanide', 'cyanides amenable to chlorination (hcn & cn)', 'dissolved arsenic', 'dissolved boron', 'dissolved selenium',
                                      'germanium', 'helium', 'iodide', 'krypton', 'neon', 'perchlorate', 'selenium', 'sulfur hexafluoride', 'tellurium', 'total arsenic', 'total boron', 'total selenium', 'xenon', 'chlorate', 'antimony, total', 'boron, total', 'asbestos']
        inorganics_minor_metals = ['aluminum', 'barium', 'beryllium', 'bismuth', 'cadmium', 'cerium', 'cesium', 'chromium', 'chromium(iii)', 'chromium(vi)', 'cobalt', 'copper', 'dissolved aluminum', 'dissolved barium', 'dissolved cadmium', 'dissolved chromium', 'dissolved copper', 'dissolved iron', 'dissolved lead', 'dissolved manganese', 'dissolved mercury', 'dissolved molybdenum', 'dissolved nickel', 'dissolved zinc', 'dysprosium', 'erbium', 'europium', 'gadolinium', 'gallium', 'holmium', 'iron', 'iron, ion (fe2+)', 'lanthanum', 'lead', 'lithium', 'lutetium', 'manganese', 'mercury', 'molybdenum', 'neodymium', 'nickel', 'niobium', 'praseodymium', 'rhenium', 'rubidium', 'samarium', 'scandium', 'silver',
                                   'strontium', 'terbium', 'thallium', 'thulium', 'tin', 'titanium', 'total aluminum', 'total barium', 'total cadmium', 'total chromium', 'total copper', 'total iron', 'total iron-d max, dmr', 'total lead', 'total manganese', 'total mercury', 'total molybdenum', 'total nickel', 'total zinc', 'tungsten', 'vanadium', 'ytterbium', 'yttrium', 'zinc', 'zirconium', 'iron, dissolved', 'chromium, hex, as cr', 'copper, free', 'iron, suspended', 'manganese, suspended', 'beryllium, total', 'bismuth, total', 'chromium, hex', 'cobalt, total', 'lithium, total', 'molybdenum, total', 'thallium, total', 'tin, total', 'titanium, total', 'vanadium, total', 'lead summary', 'copper summary', 'manganese, dissolved']
        nutrient = ['ammonia', 'ammonia and ammonium', 'ammonia as n', 'ammonia as nh3', 'ammonia-nitrogen', 'ammonia-nitrogen as n', 'ammonium', 'ammonium as n', 'dissolved nitrate: no3', 'inorganic nitrogen (nitrate and nitrite)', 'inorganic nitrogen (nitrate and nitrite) as n', 'kjeldahl nitrogen', 'nitrate', 'nitrate as n', 'nitrate-nitrogen', 'nitrite', 'nitrite as n', 'nitrogen', 'orthophosphate', 'nitrogen, ammonium/ammonia ratio', 'dissolved nitrite: no2', 'nitrogen, mixed forms (nh3), (nh4), organic, (no2) and (no3)',
                    'no2+no3 as n', 'organic nitrogen', 'ortho. phosphate', 'orthophosphate as p', 'phosphate', 'phosphate-phosphorus', 'phosphate-phosphorus as p', 'phosphate-phosphorus as po4', 'phosphorus', 'total phosphorus', 'nitrate + nitrite as n', 'phosphate, tot. dig. (as p)', 't.k.n.', 'phosphorus 0 as p', 'nitrogen-ammonia as (n)', 'nitrate-nitrite', 'phosphate, total', 'total kjeldahl nitrogen (in water mg/l)', 'phosphorus, soluble', 'phosphate, reactive', 'phosphorus, total']

        original_chemical = chemical
        chemical = chemical.lower()
        milli_per_liter = 'mg/l'

        if chemical in inorganics_major_metals and unit == 'ug/l':
            return self.calculate_amount(current_amount, 0.001), \
                milli_per_liter, original_chemical
        elif chemical in inorganics_minor_metals and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 1000), 'ug/l', \
                original_chemical
        elif (chemical in inorganics_major_nonmetals and
              unit == 'ug/l'):
            return self.calculate_amount(current_amount, 0.001), \
                milli_per_liter, original_chemical
        elif (chemical in inorganics_minor_nonmetals and
              unit == milli_per_liter):
            return self.calculate_amount(current_amount, 1000), 'ug/l', \
                original_chemical
        elif chemical in nutrient and unit == 'ug/l':
            return self.calculate_amount(current_amount, 0.001), \
                milli_per_liter, original_chemical
        elif chemical == 'nitrate' and unit == 'mg/l as n':
            return self.calculate_amount(current_amount, 4.426802887), \
                milli_per_liter, original_chemical
        elif chemical == 'nitrite' and unit == 'mg/l as n':
            return self.calculate_amount(current_amount, 3.284535258), \
                milli_per_liter, original_chemical
        elif chemical == 'phosphate' and unit == 'mg/l as p':
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, original_chemical
        elif chemical == 'bicarbonate as caco3' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, 'Bicarbonate'
        elif chemical == 'carbonate as caco3' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 0.60), \
                milli_per_liter, 'Carbonate'
        elif (chemical == 'alkalinity, bicarbonate as caco3' and
              unit == milli_per_liter):
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, 'Bicarbonate'
        elif chemical == 'bicarbonate as caco3' and unit == 'mg/l as caco3':
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, 'Bicarbonate'
        elif chemical == 'alkalinity, carbonate' and unit == 'mg/l as caco3':
            return self.calculate_amount(current_amount, 0.60), \
                milli_per_liter, 'Carbonate'
        elif chemical == 'carbonate as co3' and unit == milli_per_liter:
            return current_amount, unit, 'Carbonate'
        elif chemical == 'carbonate (co3)' and unit == milli_per_liter:
            return current_amount, unit, 'Carbonate'
        elif chemical == 'bicarbonate as hco3' and unit == milli_per_liter:
            return current_amount, unit, 'Bicarbonate'
        elif (chemical == 'alkalinity, carbonate as caco3' and
              unit == 'mg/l as caco3'):
            return self.calculate_amount(current_amount, 0.60), \
                milli_per_liter, 'Carbonate based on alkalinity'
        elif (chemical == 'alkalinity, bicarbonate' and
              unit == 'mg/l as caco3'):
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, 'Bicarbonate based on alkalinity'
        elif chemical == 'alkalinity' and unit == 'mg/l as caco3':
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, 'Bicarbonate based on alkalinity'
        elif chemical == 't.alk/caco3' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, 'Bicarbonate based on alkalinity'
        elif chemical == 'total alkalinity as caco3' and unit == 'mg/l':
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, 'Bicarbonate based on alkalinity'
        elif chemical == 'bicarbonate' and unit == 'mg/l as caco3':
            return self.calculate_amount(current_amount, 1.22), \
                milli_per_liter, original_chemical
        elif chemical == 'phosphate-phosphorus' and unit == 'mg/l as p':
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, 'Phosphate'
        elif chemical == 'phosphate-phosphorus' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, 'Phosphate'
        elif chemical == 'sulfate as s' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 0.333792756), \
                milli_per_liter, 'Sulfate'
        elif (chemical == 'nitrate-nitrogen' and unit == 'mg/l as n'):
            return self.calculate_amount(current_amount, 4.426802887), \
                milli_per_liter, 'Nitrate'
        elif chemical == 'nitrate as n' and unit == 'mg/l as n':
            return self.calculate_amount(current_amount, 4.426802887), \
                milli_per_liter, 'Nitrate'
        elif chemical == 'nitrite as n' and unit == 'mg/l as n':
            return self.calculate_amount(current_amount, 3.284535258), \
                milli_per_liter, 'Nitrite'
        elif chemical == 'nitrate-nitrogen' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 4.426802887), \
                milli_per_liter, 'Nitrite'
        elif chemical == 'nitrate as n' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 4.426802887), \
                milli_per_liter, 'Nitrate'
        elif chemical == 'nitrite as n' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 3.284535258), \
                milli_per_liter, 'Nitrite'
        elif ((chemical == 'nitrate-nitrite' or
               chemical == 'inorganic nitrogen (nitrate and nitrite) as n' or
               chemical == 'nitrate + nitrate as n' or
               chemical == 'no2+no3 as n') and
              (unit == 'mg/l as n' or unit == milli_per_liter)):
            return self.calculate_amount(current_amount, 4.426802887), \
                milli_per_liter, 'Nitrate and nitrite as no3'
        elif chemical == 'phosphate-phosphorus as p' and unit == 'mg/l as p':
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, 'Phosphate'
        elif chemical == 'orthophosphate as p' and unit == 'mg/l as p':
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, 'Phosphate'
        elif (chemical == 'phosphate-phosphorus as p' and
              unit == milli_per_liter):
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, 'Phosphate'
        elif chemical == 'orthophosphate as p' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, 'Phosphate'
        elif (chemical == 'orthophosphate' and unit == 'mg/l as p'):
            return self.calculate_amount(current_amount, 3.131265779), \
                milli_per_liter, 'Phosphate'
        elif chemical == 'ammonia and ammonium' and unit == 'mg/l nh4':
            return self.calculate_amount(current_amount, 1.05918619), \
                milli_per_liter, 'Ammonia'
        elif chemical == 'ammonia-nitrogen as n' and unit == 'mg/l as n':
            return self.calculate_amount(current_amount, 1.21587526), \
                milli_per_liter, 'Ammonia'
        elif chemical == 'ammonia-nitrogen' and unit == 'mg/l as n':
            return self.calculate_amount(current_amount, 1.21587526), \
                milli_per_liter, 'Ammonia'
        elif chemical == 'ammonia-nitrogen as n' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 1.21587526), \
                milli_per_liter, 'Ammonia'
        elif chemical == 'ammonia-nitrogen' and unit == milli_per_liter:
            return self.calculate_amount(current_amount, 1.21587526), \
                milli_per_liter, 'Ammonia'
        elif chemical == 'ammonia' and unit == 'mg/l as n':
            return self.calculate_amount(current_amount, 1.21587526), \
                milli_per_liter, original_chemical
        elif chemical == 'specific conductance' and unit == 'ms/cm':
            return self.calculate_amount(current_amount, 1000), 'uS/cm', \
                original_chemical
        elif chemical == 'specific conductance' and unit == 'umho/cm':
            return current_amount, 'uS/cm', original_chemical
        elif chemical == 'calcium' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 20.039), \
                milli_per_liter, original_chemical
        elif chemical == 'magnesium' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 12.1525), \
                milli_per_liter, original_chemical
        elif chemical == 'potassium' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 39.0983), \
                milli_per_liter, original_chemical
        elif chemical == 'sodium' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 22.9897), \
                milli_per_liter, original_chemical
        elif chemical == 'nitrate' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 62.0049), \
                milli_per_liter, original_chemical
        elif chemical == 'chloride' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 35.453), \
                milli_per_liter, original_chemical
        elif chemical == 'hydroxide' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 17.0073), \
                milli_per_liter, original_chemical
        elif chemical == 'sulfate' and unit == 'ueq/l':
            return self.calculate_amount(current_amount, 24.01565), \
                milli_per_liter, original_chemical
        else:
            return current_amount, unit, original_chemical

    def calculate_amount(self, amount, conversion_rate):
        if amount is None:
            return None
        elif not amount:
            return 0

        return amount * conversion_rate

    def calculate_paramgroup(self, chemical):
        if chemical in self.paramgroup:
            paramgroup = self.paramgroup[chemical]
            return paramgroup


class ChargeBalancer(object):

    """https://github.com/agrc/ugs-chemistry/issues/22"""

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

    def __init__(self):
        super(ChargeBalancer, self).__init__()

    def calculate_charge_balance(self, concentration):
        calcium = self._conversions['ca'] * (concentration.calcium or 0)
        magnesium = self._conversions['mg'] * (concentration.magnesium or 0)
        sodium = self._conversions['na'] * (concentration.sodium or 0)
        potassium = self._conversions['k'] * (concentration.potassium or 0)
        chloride = self._conversions['cl'] * (concentration.chloride or 0)
        bicarbonate = self._conversions[
            'hco3'] * (concentration.bicarbonate or 0)
        sulfate = self._conversions['so4'] * (concentration.sulfate or 0)
        carbonate = self._conversions['co3'] * (concentration.carbonate or 0)
        nitrate = self._conversions['no3'] * (concentration.nitrate or 0)
        nitrite = self._conversions['no2'] * (concentration.nitrite or 0)
        sodium_plus_potassium = self._conversions[
            'na+k'] * (concentration.sodium_plus_potassium or 0)

        cation = sum(
            [calcium, magnesium, sodium, potassium, sodium_plus_potassium])
        anion = sum(
            [chloride, bicarbonate, carbonate, sulfate, nitrate, nitrite])

        balance = 100 * float((cation - anion) / (cation + anion))

        # returns charge balance (%), cation total (meq/l), anion total (meq/l)
        return round(balance, 2), round(cation, 2), round(anion, 2)
