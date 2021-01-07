用于测试redis-replicator delay

1、java -jar replicator-daley-test.jar  ${Tair B实例地址}  ${Tair B实例端口}   ${Tair B实例密码}   ${需要读取的key-value对数目}  get；该命令向Tair拉取数据

2、java -jar replicator-daley-test.jar  ${Tair A实例地址}  ${Tair A实例端口}   ${Tair A实例密码}  ${需要写入的key-value对数目}  set；该命令向Tair插入数据
