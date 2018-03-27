OpsManCombo


OpsManCombo is CLI tool for MongoDB database cluster and  single node management  via Ops Manager REST API,
designed for technical teams. More information about Ops Manager can be found under:
https://docs.opsmanager.mongodb.com/current/, information about Ops Manager REST API can be found under:
https://docs.opsmanager.mongodb.com/current/reference/api/

Opsmancombo use Ops Manager REST API calls for MongoDB database minor and major upgrade, downgrade, MongoDB cluster alert detection, replication check, stoping and starting MongoDB processess on single node. 

OpsManCombo should be used by your Dev Ops team! - OpsManCombo can be integrated with frameworks like Ansible!.
OpsManCombo can be started in upgrade (for MongoDB databases upgrade) and maintenance mode (for cluster and single nodes maintenance).
OpsManCombo can use SSL Cert Verification or can be run without SSL Cert Verification. Logging option can also be set.

use cases:
- MongoDB cluster upgrade,
- MongoDB cluster downgrade,
- stop MongoDB processess on one node from MongoDB cluster (e.g for node reboot or HW exchange),
- start MongoDB processess on MongoDB node,
- alert detection on MongoDB cluster,
- replication sync check on MongoDB cluster.

Maintenance mode is recomended for technical staff for their day to day tasks - like take one node out from cluster for
firmware  upgrade and reboot or kernel patching. Before any operation maintenance window will be created on Ops Manager to avoid alert generation. After operation maintenance window will be deleted.



MongoDB database upgrade examples:
 - MongoDB cluster upgrade with SSL Cert Verification:

opsman.py -u <ops_manager_user_name> -k <ops_manager_api_key> -m https://<ops_manager:port> 
 -l <path_to_logs> --verify <path_to_certificate> upgrade 
 -d <database_name> -v <desired_MongoDB_version> 


 - MongoDB cluster upgrade without SSL Cert Verification:
opsman.py -u <ops_manager_user_name> -k <ops_manager_api_key> -m https://<ops_manager:port> -l <path_to_logs> 
 --no-verify upgrade 
 -d <database_name> -v <desired_MongoDB_version> 



OpsManCombo can be also start in maintenance mode for single node from the cluster.
to check replication sync:
./opsman.py -u <ops_manager_user_name> -k <ops_manager_api_key> -m https://<ops_manager:port> -l <path_to_logs> 
 --no-verify maintenance -a sync -n <node_name> 

to check alerts on MongoDB cluster:
./opsman.py -u <ops_manager_user_name> -k <ops_manager_api_key> -m https://<ops_manager:port> -l <path_to_logs> 
 --no-verify maintenance -a alert -n <node_name>

to stop mongoDB processes on single node:
./opsman.py -u <ops_manager_user_name> -k <ops_manager_api_key> -m https://<ops_manager:port> -l <path_to_logs> 
 --no-verify maintenance -a stop -n <node_name> 

to start mongoDB processes on single node:
./opsman.py -u <ops_manager_user_name> -k <ops_manager_api_key> -m https://<ops_manager:port> -l <path_to_logs> 
 --no-verify maintenance -a start -n <node_name>

to check if any change is currently  deploying on MongoDB cluster by Ops Manager
./opsman.py -u <ops_manager_user_name> -k <ops_manager_api_key> -m https://<ops_manager:port> -l <path_to_logs> 
 --no-verify maintenance -a check -n <node_name>


input parameters:

upgrade mode:
-u <user name - mainly e-mail address>
-k <api key from Ops Manager>
-m <ops manager address>:<port>
-l <path to log file>
ssl: --no-verify  | --verify <path to ssl certificate> 
-d <MongoDB database name>
-v <desired MongoDB_version>

maintenance mode:
-u <user_name:e-mail_address>
-k <api_key_from_OpsManager>
-m <ops_manager_address:port>
-l <path_to_log_file>
ssl: --no-verify  | --verify <path to ssl certificate>
-a <action:alert,stop,start,sync,check>
-n <single_node_address>
