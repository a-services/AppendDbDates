package io.appery.dbdates

import groovy.io.FileType

/** 
 * Append `_createdAt` and `_updatedAt` dates to CSV files exported from Appery DB. 
 */
class CsvProcessor {
    
    String csvExportFolder;
    String csvOutputFoler;
    String dbId;
    String masterKey;
    String apperyUser;
    String apperyPassword;
    
    List rows;
    Writer out;
    ApperyDbClient apperyDbClient;
    
    void processExportedDatabase(String propName) {
        println '-'*80
        
        // Getting properties from property-file
        Properties pp = loadProperties(propName)
        csvExportFolder = getProperty(pp, 'csv_export_folder')
        csvOutputFoler = getProperty(pp, 'csv_output_foler')
        dbId = getProperty(pp, 'db_id')
        masterKey = getProperty(pp, 'master_key')

        apperyDbClient = new ApperyDbClient(dbId, masterKey)
        
        new File(csvExportFolder).eachFile FileType.FILES, { f ->
            if (!f.name.endsWith('.csv')) {
                println "   Skipping " + f.name
            } else {
                List text = f.readLines()
                if (text.size() == 0) {
                    throw new ApperyDbException("Missing header in CSV file")
                }
                String header = text[0]
                String newColumnNames = ',"_createdAt:date","_updatedAt:date"'
                if (header.endsWith(newColumnNames)) {
                    throw new ApperyDbException("CSV files in this folder already updated with _createdAt / _updatedAt dates")
                }
                if (!header.startsWith('"_id:string",')) {
                    throw new ApperyDbException("We expect first column in CSV file to be `_id`")
                }
                out = new File(csvOutputFoler, f.name).newWriter()
                try {
                    out.println header + newColumnNames
                    if (text.size() > 1) {
                        updateCsvCollection(f.name, text.subList(1, text.size()))
                    }
                } finally {
                    out.close()
                }
            }
        }
        
        println "="*80
    }

    String getProperty(Properties pp, String name) {
        String value = pp.getProperty(name)
        if (value == null) {
            throw new ApperyDbException("Property required: " + name)
        }
        return value
    }
    
    Properties loadProperties(String propName) {
        Properties pp = new Properties();
        FileInputStream fin = new FileInputStream(propName);
        pp.load(fin);
        fin.close();
        return pp;	    
    }
    
    String noExt(String collName) {
        return collName.substring(0, collName.length()-4)
    }

    void updateCsvCollection(String collName, List collData) {
        println "...Updating collection: " + collName
        List jsonList = apperyDbClient.getCollection(noExt(collName))
        boolean isUsers = collName == '_users.csv'
        for (String s: collData) {
            int k = s.indexOf(',')
            if (k < 0) {
                throw new ApperyDbException("We expect first column in CSV file to be `_id`");
            }
            String id = s.substring(0,k)
            if (!(id.startsWith('"') && id.endsWith('"'))) {
                throw new ApperyDbException("We expect first column in CSV file to be `_id`");
            }
            id = id.substring(1, id.length() - 1)
            def rec = jsonList.find { it._id == id }
            
            if (rec == null) {
                throw new ApperyDbException("_id `$id` missing in cloud collection `${noExt(collName)}`")
            }
            String createdAt = rec._createdAt
            String updatedAt = rec._updatedAt
            out.println s + ',"' + createdAt + '","' + updatedAt + '"'
        }
    }
    
}
