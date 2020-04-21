from elasticmem import ElasticMemClient

host = "ec2-54-244-209-229.us-west-2.compute.amazonaws.com"
path = "test"
em = ElasticMemClient(host=host)

kv = em.open_or_create(path, "/tmp", 10)
puts = kv.pipeline_put()
for i in range(10):
    puts.put(str(i), str(i))
puts.execute()
em.close(path)
