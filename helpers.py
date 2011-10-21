import pickle


def decode_tuple_data(data):
    return pickle.loads(data)

def encode_tuple(t):
    return pickle.dumps(t)
