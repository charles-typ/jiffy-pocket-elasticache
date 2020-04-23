import redis
#from redis import StrictRedis


hostname = "ec2-54-244-209-229.us-west-2.compute.amazonaws.com"
r = redis.Redis(host=hostname, port=6379, db=0)
r.set('foo', 'bar')
r.get('foo')
#path = "test"
#rs = StrictRedis(host=hostname, port=6379, db=0).pipeline()
#print("Connected to strict redis")
#
#
#for i in range(10):
#    rs.set(str(i), str(i))
#print("Finish putting the key and values")
#rs.execute()
#rs.close(path)
