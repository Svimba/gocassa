# gocassa


 Commands:
         help                       Print application usage
         info <id>                  Returns base information about object with <ID>, stored in DB 
         fulltext <string>          Returns all records which contains <string>
         check-backref <id>         Check back reference inconsistency for <id> or all ids
         check-all-backref all      Check back reference inconsistency for <id> or all ids


 Flags:
  -keyspace string
        Cassandra KeySpace (default "config_db_uuid")
  -port int
        Cassandra port (default 9041)
  -server string
        Server IP address (default "127.0.0.1")

Examples:
---------

$ gocassa --server 10.10.11.10 --port 9042 check-all-backref all

$ gocassa --server 10.10.11.10 --port 9042 check-backref 5434d705-e9b1-430b-889f-8edff31cf62f

$ gocassa --server 10.10.11.10 --port 9042 info 5434d705-e9b1-430b-889f-8edff31cf62f
