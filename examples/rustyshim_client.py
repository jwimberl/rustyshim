import pyarrow.flight as pf

class RustyShimConnection:
    class AuthHandler(pf.ClientAuthHandler):
        def __init__(self, username, password):
            self.username = username
            self.password = password
            self.token = None
        def authenticate(self, outgoing, incoming):
            outgoing.write(self.username)
            outgoing.write(self.password)
            self.token = incoming.read()
        def get_token(self):
            if self.token is None:
                raise pf.FlightUnauthenticatedError("Not authenticated via SciDB")
            return self.token
    
    def __init__(self, location, username, password):
        self.client = pf.connect(location)
        h = RustyShimConnection.AuthHandler(username, password)
        self.client.authenticate(h)
        self.options = pf.FlightCallOptions(headers=[(b'authorization',h.get_token())])
    
    def get_sql(self, query):
        fd = pf.FlightDescriptor.for_path(query)
        fi = self.client.get_flight_info(fd, self.options)
        ep = fi.endpoints[0]
        return(self.client.do_get(ep.ticket, self.options))

def rustyshim_connect(location, username, password):
    return(RustyShimConnection(location, username, password))