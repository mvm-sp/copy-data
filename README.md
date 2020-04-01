#To Run the export method
node read-from-clinux <client number>
#To Rum the import/reading/process
node write-to-nest
#To Package
npm install -g pkg@4.3.7
pkg  read-from-clinux.js -o read-from-clinux.exe [--targets=node10-win-x64]
pkg  write-to-nest.js -o write-to-nest.exe [--targets=node10-win-x64]
pkg  generate-nest-kpi.js -o generate-nest-kpi.exe [--targets=node10-win-x64]
pkg  write-ans-to-nest.js -o write-ans-to-nest.exe [--targets=node10-win-x64]