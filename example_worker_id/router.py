import zmq
import codecs
import dill


def serialize(obj: object) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(ser_obj: str) -> object:
    return dill.loads(codecs.decode(ser_obj.encode(), "base64"))


router =  zmq.Context().socket(zmq.ROUTER)
router.bind("tcp://0.0.0.0:8001")

while True:
    worker, message = router.recv_multipart()
                                          
    message = deserialize(message.decode('utf-8'))
    
    #router.send_multipart([worker, serialize("OK").encode('utf-8')])
    router.send_multipart([worker, worker])


# 'gASVTwAAAAAAAAB9lCiMB3Rhc2tfaWSUjAMxMjOUjApmbl9wYXlsb2FklIwWZGVmIGZuKHgpOiBy\nZXR1cm4geCoqMpSMDXBhcmFtX3BheWxvYWSUjAEylHUu\n'