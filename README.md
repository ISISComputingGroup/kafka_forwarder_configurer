# kafka_forwarder_configurer

This process listens to the [blockserver](https://github.com/ISISComputingGroup/EPICS-inst_servers) and the mysql instance on instruments to configure the [forwarder](https://github.com/ISISComputingGroup/forwarder) to start streaming PV updates. 

Currently these PVs are: 
- blocks
- some `runlog` values from the DAE
- archived PVs

  
