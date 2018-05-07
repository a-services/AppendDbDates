package io.appery.dbdates

propName = 'AppendDbDates.properties'
if (args.length > 0) {
    propName = args[0]
}
try {
    new CsvProcessor().processExportedDatabase(propName)
} catch(ApperyDbException e) {
    println "[ERROR] ${e.reason}"
}