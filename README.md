#AlgoPicker
## How to run
1. install Nodejs V6.9.1
2. install NodeRed: sudo npm install -g node-red
3. replace $NODE_HOME/lib/node_modules/node-red/api/flows.js with our modified flows.js
4. put nodes directory to ~/.node-red
5. run Node-red: node-red
6. run algoPicker: java -Dserver.port=8001 -Dtaql.spark.home="/home/scidb/spark-2.0.0" -jar algo.picker-1.0-SNAPSHOT.jar