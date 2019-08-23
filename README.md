#To Run the export method
node read-from-clinux <client number>
#To Rum the import/reading/process
node write-to-nest
#To Package
npm install -g pkg@4.3.7
pkg  read-from-clinux.js -o read-from-clinux.exe
pkg  write-to-nest.js -o write-to-nest.exe
pkg  generate-nest-kpi.js -o generate-nest-kpi.exe