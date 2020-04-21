from redis import StrictRedis

hostname = "ec2-54-244-209-229.us-west-2.compute.amazonaws.com"
path = "test"
rs = StrictRedis(host=hostname, port=6379, db=0).pipeline()

for i in range(10):
    rs.set(str(i), str(i))
rs.execute()
em.close(path)
