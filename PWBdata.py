class PWBdata:
    """
    Utility class for the PWB data
    """
    def __init__(self, hdfs_path):
        self.hdfs_path = hdfs_path
        self.resnet_files = ['elsevier-csv-resnet-annotation','elsevier-csv-resnet-binding','elsevier-csv-resnet-biomarker','elsevier-csv-resnet-cellexpression',\
                         'elsevier-csv-resnet-cellobject','elsevier-csv-resnet-cellprocess','elsevier-csv-resnet-celltype','elsevier-csv-resnet-chemicalreaction',\
                         'elsevier-csv-resnet-clinicalparameter','elsevier-csv-resnet-clinicaltrial','elsevier-csv-resnet-complex','elsevier-csv-resnet-dictvaluetype',\
                         'elsevier-csv-resnet-directregulation','elsevier-csv-resnet-disease','elsevier-csv-resnet-expression','elsevier-csv-resnet-functionalassociation',\
                         'elsevier-csv-resnet-functionalclass','elsevier-csv-resnet-geneticchange','elsevier-csv-resnet-geneticvariant','elsevier-csv-resnet-group',\
                         'elsevier-csv-resnet-mirnaeffect','elsevier-csv-resnet-molsynthesis','elsevier-csv-resnet-moltransport','elsevier-csv-resnet-organ',\
                         'elsevier-csv-resnet-organism','elsevier-csv-resnet-other','elsevier-csv-resnet-pathway','elsevier-csv-resnet-promoterbinding',\
                         'elsevier-csv-resnet-propertytype','elsevier-csv-resnet-protein','elsevier-csv-resnet-protmodification','elsevier-csv-resnet-quantitativechange',\
                         'elsevier-csv-resnet-regulation','elsevier-csv-resnet-semanticconcept','elsevier-csv-resnet-smallmol','elsevier-csv-resnet-statechange',\
                         'elsevier-csv-resnet-submission','elsevier-csv-resnet-tissue','elsevier-csv-resnet-treatment','elsevier-csv-resnet-virus']

        self.resnet_data_path = self.hdfs_path + '/resnet/' 
        
        self.pp_files = ['elsevier-csv-ppplus-activitytarget',
              'elsevier-csv-ppplus-activitytargetextended',
              'elsevier-csv-ppplus-activitytargetproperty', 'elsevier-csv-ppplus-concomitant',
              'elsevier-csv-ppplus-document', 'elsevier-csv-ppplus-dosingregimen',
              'elsevier-csv-ppplus-drug', 'elsevier-csv-ppplus-drugclass',
              'elsevier-csv-ppplus-drugindication', 'elsevier-csv-ppplus-drugreference',
              'elsevier-csv-ppplus-drugtarget', 'elsevier-csv-ppplus-effect',
              'elsevier-csv-ppplus-endpoint',
              'elsevier-csv-ppplus-faersadministrativeinformation',
              'elsevier-csv-ppplus-faersdrugreport', 'elsevier-csv-ppplus-indication',
              'elsevier-csv-ppplus-measure', 'elsevier-csv-ppplus-medatatype',
              'elsevier-csv-ppplus-meenzymetransporter', 'elsevier-csv-ppplus-meyler',
              'elsevier-csv-ppplus-pathogen', 'elsevier-csv-ppplus-pkparameter',
              'elsevier-csv-ppplus-reference', 'elsevier-csv-ppplus-result',
              'elsevier-csv-ppplus-source', 'elsevier-csv-ppplus-species',
              'elsevier-csv-ppplus-study', 'elsevier-csv-ppplus-studydesign',
              'elsevier-csv-ppplus-studydesigntype', 'elsevier-csv-ppplus-subject',
              'elsevier-csv-ppplus-synonym', 'elsevier-csv-ppplus-target',
              'elsevier-csv-ppplus-word']

        self.pp_data_path = hdfs_path + '/ppplus/' 
        
        self.rmc_files = ['elsevier-csv-reaxys-activitysite', 'elsevier-csv-reaxys-administrationroute',
              'elsevier-csv-reaxys-administrationtype',
              'elsevier-csv-reaxys-affiliationorganisation',
              'elsevier-csv-reaxys-agentconfiguration', 'elsevier-csv-reaxys-aminoacid',
              'elsevier-csv-reaxys-analyticaltechnique', 'elsevier-csv-reaxys-assaycategory',
              'elsevier-csv-reaxys-author', 'elsevier-csv-reaxys-bindingexperimenttype',
              'elsevier-csv-reaxys-bioassay', 'elsevier-csv-reaxys-bioassaygraft',
              'elsevier-csv-reaxys-biologicalactivity',
              'elsevier-csv-reaxys-biologicalmaterial',
              'elsevier-csv-reaxys-biologicalmaterialtype',
              'elsevier-csv-reaxys-biologicalphenomenon',
              'elsevier-csv-reaxys-cellculturetype', 'elsevier-csv-reaxys-cellcultureuse',
              'elsevier-csv-reaxys-celltype', 'elsevier-csv-reaxys-cellularorganelle',
              'elsevier-csv-reaxys-cellularphenomenon',
              'elsevier-csv-reaxys-chemicalcompound',
              'elsevier-csv-reaxys-chemicalcompoundeffect',
              'elsevier-csv-reaxys-chemicalcompoundname',
              'elsevier-csv-reaxys-chemicalcompoundstructure',
              'elsevier-csv-reaxys-chemicalcompoundtype',
              'elsevier-csv-reaxys-citationaffiliation', 'elsevier-csv-reaxys-citationauthor',
              'elsevier-csv-reaxys-citationpatentbibliography',
              'elsevier-csv-reaxys-citationsourcetype', 'elsevier-csv-reaxys-citationtype',
              'elsevier-csv-reaxys-clamptype', 'elsevier-csv-reaxys-clinicalstate',
              'elsevier-csv-reaxys-clinicalstatetype',
              'elsevier-csv-reaxys-controldatapointtype', 'elsevier-csv-reaxys-countrycode',
              'elsevier-csv-reaxys-datasource', 'elsevier-csv-reaxys-developmentstage',
              'elsevier-csv-reaxys-document', 'elsevier-csv-reaxys-documentdetails',
              'elsevier-csv-reaxys-documenttype', 'elsevier-csv-reaxys-druglikeness',
              'elsevier-csv-reaxys-electrophysiologyconfiguration',
              'elsevier-csv-reaxys-elementlocation', 'elsevier-csv-reaxys-elementnature',
              'elsevier-csv-reaxys-experimentalmodel', 'elsevier-csv-reaxys-feeding',
              'elsevier-csv-reaxys-feedingmethod', 'elsevier-csv-reaxys-feedingtype',
              'elsevier-csv-reaxys-genotype', 'elsevier-csv-reaxys-genotypetype',
              'elsevier-csv-reaxys-highestclinicalphase',
              'elsevier-csv-reaxys-internationalchemicalidentifier',
              'elsevier-csv-reaxys-isolatedorgantype', 'elsevier-csv-reaxys-journal',
              'elsevier-csv-reaxys-journaltitle', 'elsevier-csv-reaxys-language',
              'elsevier-csv-reaxys-location', 'elsevier-csv-reaxys-measuredobject',
              'elsevier-csv-reaxys-measuredparameter', 'elsevier-csv-reaxys-measurelocation',
              'elsevier-csv-reaxys-measuresettings', 'elsevier-csv-reaxys-membrane',
              'elsevier-csv-reaxys-metabolicreaction', 'elsevier-csv-reaxys-metabolite',
              'elsevier-csv-reaxys-metabolizer', 'elsevier-csv-reaxys-metabolizertype',
              'elsevier-csv-reaxys-mutationtype', 'elsevier-csv-reaxys-organismtype',
              'elsevier-csv-reaxys-othercitationdetails', 'elsevier-csv-reaxys-otherisbn',
              'elsevier-csv-reaxys-patentassignee', 'elsevier-csv-reaxys-patentdetails',
              'elsevier-csv-reaxys-physicalstimulant', 'elsevier-csv-reaxys-precision',
              'elsevier-csv-reaxys-promoter', 'elsevier-csv-reaxys-protein',
              'elsevier-csv-reaxys-proteinfragment', 'elsevier-csv-reaxys-proteinmutation',
              'elsevier-csv-reaxys-proteinsubunit',
              'elsevier-csv-reaxys-proteinsubunitpromoter',
              'elsevier-csv-reaxys-pxvalueorigin',
              'elsevier-csv-reaxys-referencejournaldetails',
              'elsevier-csv-reaxys-relatedconcept', 'elsevier-csv-reaxys-secondmessengertype',
              'elsevier-csv-reaxys-sourcereference', 'elsevier-csv-reaxys-species',
              'elsevier-csv-reaxys-substancetype', 'elsevier-csv-reaxys-supplier',
              'elsevier-csv-reaxys-target', 'elsevier-csv-reaxys-targetnature',
              'elsevier-csv-reaxys-targetrole', 'elsevier-csv-reaxys-targetstructuralelement',
              'elsevier-csv-reaxys-therapeuticdomain', 'elsevier-csv-reaxys-tissueororgan',
              'elsevier-csv-reaxys-tissueportion', 'elsevier-csv-reaxys-transfectiontype',
              'elsevier-csv-reaxys-tumor']
        
        self.rmc_data_path = hdfs_path + '/reaxys/'
    
    def get_table_list(self, database):
        """
        given a database, return the table names and fully-qualified locations
        """
        if database == 'resnet':
            datasets = [{'file': self.resnet_data_path + f + '_deduplicated' +'/*.csv', 'name' : f[f.rindex('-')+1:].title()} for f in self.resnet_files]
        elif database == 'rmc':
            datasets = [{'file': self.rmc_data_path + f + '_deduplicated' +'/*.csv', 'name' : f[f.rindex('-')+1:].title()} for f in self.rmc_files]
        elif database == 'pp':
            datasets = [{'file': self.pp_data_path + f + '_deduplicated' +'/*.csv', 'name' : f[f.rindex('-')+1:].title()} for f in self.pp_files]
        elif database == 'chembl':
            datasets = [{'file': self.chembl_data_path + f + '_deduplicated' +'/*.csv', 'name' : f[f.rindex('-')+1:].title()} for f in self.resnet_files]
        else:
            print (database + 'not found')
            return None
        return datasets
                
    def get_dataframe(self, sparkSession, database, table_name):
        """
        fetch a spark dataframe for a table in a database
        """
        if database == 'resnet':
            data_path = self.resnet_data_path
            stem = 'elsevier-csv-resnet-'
        elif database == 'rmc':
            data_path = self.rmc_data_path
            stem = 'elsevier-csv-reaxys-'
        elif database == 'pp':
            data_path = self.pp_data_path
            stem = 'elsevier-csv-ppplus-'
        elif database == 'chembl':
            data_path = self.chembl_data_path
            stem = 'elsevier-csv-chembl-'
        filename = data_path + stem + table_name.lower() + '_deduplicated' +'/*.csv'
        df = sparkSession \
            .read \
            .option("header","true") \
            .csv(filename)
        return df
