# TPE2-G9

## Compilacion
Parado en la carpeta del proyecto ejecutar los siguientes comandos:
* maven clean install
* cd client/target
* tar -xzf tpe2-g9-client-1.0-SNAPSHOT-bin.tar.gz
* cd tpe2-g9-client-1.0-SNAPSHOT
* chmod u+x query*
* cd ../../../server/target
* tar -xzf tpe2-g9-server-1.0-SNAPSHOT-bin.tar.gz
* cd tpe2-g9-server-1.0-SNAPSHOT
* chmod u+x run-server.sh

### Ejecutar el server
Parado en la carpeta del proyecto ejecutar  
`cd server/target/tpe2-g9-server-1.0-SNAPSHOT`  
luego el comando  
`./run-server.sh [-Daddress=xxx.xxx.xxx.*] [-Dmanagement=1] `  
Parametros opcionales:  
-Daddress: Configurar el address. El default es 192.168.0.*  
-Dmanagement=1: Si uno desea utilizar un management center.
Para el management center la url configurada es: http://localhost:8080/mancenter_3_8_5/

### Ejecutar las queries
Parado en la carpeta del proyecto ejecutar  
`cd client/target/tpe2-g9-server-1.0-SNAPSHOT`  
`./queryX -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX -DoutPath=YY [params] [-Dc=1]`  

queryX es el script que corre la query X.  
-Daddresses refiere a las direcciones IP de los nodos con sus puertos 
(una o más, separadas por punto y coma)  
-DinPath indica el path donde están los archivos de entrada 
bikes.csv y stations.csv.  
-DoutPath indica el path donde estarán ambos archivos de salida query1.csv y time1.txt.  
`-Dc=1` es para usar el combiner

#### Query 1 
Sin parametros adicionales
#### Query 2
`-Dn=x` x= limite de cantidad de resultados (numero entero)
#### Query 3
Sin parametros adicionales
#### Query 4
`-DstartDate` la fecha de inicio del rango en formato DD/MM/YYYY
`-DendDate` la fecha de fin del rango en formato DD/MM/YYYY